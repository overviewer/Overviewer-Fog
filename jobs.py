#!/usr/bin/python2

import boto
from uuid import uuid4 as gen_uuid
import time

__all__ = ["job_field", "Job"]

# various status codes, found at job.status
SUBMITTED = "submitted"
INPROGRESS = "inprogress"
COMPLETE = "complete"
ERROR = "error"


def job_field(name, default=None, convert=None):
    """Create a job field description. These are used inside the
    `job_fields` list in each Job subclass. Each field has a name, and
    possibly a default and converter function. The default value is
    used for the field whenever a value isn't provided; if the default
    value is callable, it is called with no agruments to get the true
    default. A default of None represents a required field. A
    converter function is used to make sure that values loaded out of
    the backend have the type expected; usually it's something simple,
    like `int` or `float`.
    """
    if default is not None and not callable(default):
        get_default = lambda: default
    else:
        get_default = default
    return (name, get_default, convert)


class Job(object):
    """This Job class can be subclassed to represent a certain type of
    distributed job. Subclasses should set the class variables
    `job_type` and `job_fields`, and may also add helper
    methods. `job_type` should be set to a string unique to the job
    type; it is used to name backend databases and such. `job_fields`
    should be a list of strings or tuples returned by `job_field`, and
    each one represents a piece of data associated with each job. In
    instances of these jobs, the data can be accessed at
    `job.field_name`.

    The base Job class includes some default fields that are
    automatically managed by Job; these are `submitted`, `completed`,
    `status`, and `errmsg`. `submitted` is automatically set to the
    unix time when the job is created, and `completed` is set to the
    unix time when `finish()` or `error(...)` are called. `status` is
    set to one of SUBMITTED, INPROGRESS, COMPLETE, or ERROR by the job
    automatically based on its status, and `errmsg` is set to a
    helpful message by `error(...)` at the same time it sets `status`
    to ERROR.

    The following example is one of the simplest Job subclasses possible:

        class ExampleJob(Job):
            job_type = "example"
            job_fields = [
                "required_field",
                job_field("optional_field", 42, int),
            ]

    """
    # configurable in subclasses
    job_type = "job"
    job_fields = []

    # probably shouldn't be configured in subclasses
    job_type_prefix = "fog-"
    job_fields_internal = [
        job_field('submitted', lambda: time.time(), float),
        job_field('completed', 0, float),
        job_field('status', SUBMITTED, str),
        job_field('errmsg', ''),
    ]

    # AWS connection cache
    job_queue = None
    job_database = None

    def __init__(self, uuid, message, data):
        """Create a job object. This is probably not what you're
        looking for; look at `sumbit(...)` and the `fetch_*` family of
        functions.
        """
        self.uuid = uuid
        self.message = message
        self.data = {}

        # sanity check for shadowed member variables
        for k in self.job_fields:
            try:
                k, _, _ = k
            except ValueError:
                pass

            if k in dir(self):
                raise ValueError("job field '{0}' shadows a member variable".format(k))
            for j, _, _ in self.job_fields_internal:
                if k == j:
                    raise ValueError("job field '{0}' shadows an internal job field".format(k))

        # ok, now set data
        self.set_data(data)

    def __repr__(self):
        # a nice representation for debugging
        return "<{0} {1}>".format(self.__class__.__name__, repr(self.data))

    def __getattr__(self, name):
        # handle getting our job fields

        # shim for before data is set
        if name == 'data':
            return {}

        try:
            return self.data[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        # handle setting our job fields
        if not name in self.data:
            super(Job, self).__setattr__(name, value)
        else:
            self.data[name] = value

    def update(self, visibility_timeout=60):
        """Update the job in the backend. You should call this after
        changing the values of any of the job fields, so that these
        changes will show up on other machines. If you got this job
        from `fetch_next()`, this also prevents other machines from
        seeing this job for `visibility_timeout` seconds.
        """
        if self.message:
            self.message.change_visibility(visibility_timeout=visibility_timeout)
        self._open_queues()
        success = self.job_database.put_attributes(self.uuid, self.get_data())
        if not success:
            raise RuntimeError("could not put key '{0}' in SDB domain '{1}'".format(self.uuid, self.job_database.name))

    def finish(self):
        """Mark the job as finished. Sets `status` to COMPLETE and
        sets the time in `completed` to the current time. This also
        implicitly calls `update()`.

        You cannot `finish()` a job unless you got it from `fetch_next()`.
        """
        if not self.message:
            raise RuntimeError("you cannot finish() a job unless you get it from fetch_next()")
        success = self.message.delete()
        if not success:
            raise RuntimeError("could not delete message '{0}' from SQS queue '{1}'".format(self.uuid, self.job_queue.name))
        self.message = None

        self.status = COMPLETE
        self.completed = time.time()
        self.update()

    def error(self, message):
        """Mark a job as finished, with an error message. Sets
        `status` to ERROR, sets the time in `completed` to the current
        time, and sets `errstr` to the given message. This also
        implicitly calls `update()`.

        You cannot `error(...)' a job unless you got it from
        `fetch_next()`.
        """
        if not self.message:
            raise RuntimeError("you cannot error() a job unless you get it from fetch_next()")
        success = self.message.delete()
        if not success:
            raise RuntimeError("could not delete message '{0}' from SQS queue '{1}'".format(self.uuid, self.job_queue.name))
        self.message = None

        self.status = ERROR
        self.completed = time.time()
        self.errmsg = message
        self.update()

    def delete(self):
        """Delete a job. This prevents the job from showing up
        anywhere, including `fetch_all()` and
        `fetch_all_completed()`. Note that currently, you cannot
        delete a job unless `status` is COMPLETE or ERROR.
        """
        if self.status not in [COMPLETE, ERROR]:
            raise RuntimeError("you cannot delete job '{0}' because it is still pending".format(self.uuid))
        if self.message:
            success = self.message.delete()
            if not success:
                raise RuntimeError("could not delete message '{0}' from SQS queue '{1}'".format(self.uuid, self.job_queue.name))
            self.message = None
        self._open_queues()
        success = self.job_database.delete_attributes(self.uuid)
        if not success:
            raise RuntimeError("could not delete key '{0}' from SDB domain '{1}'".format(self.uuid, self.job_database.name))

    def set_data(self, data):
        """Sets the fields according to a dictionary in `data`, that
        was created by `get_data`, possibly on another machine. If you
        want to change the internal representation used by your job
        class, you should override this method and its twin
        `get_data`.
        """
        self.data = {}
        for key in self.job_fields + self.job_fields_internal:
            try:
                key, get_default, convert = key
            except ValueError:
                get_default = None
                convert = None

            if not key in data:
                if not get_default:
                    raise ValueError("passed data does not have required key '{0}'".format(key))
                else:
                    try:
                        self.data[key] = get_default()
                    except Exception:
                        print "got exception in default handler for '{0}'".format(key)
                        raise
            else:
                if convert:
                    try:
                        self.data[key] = convert(data[key])
                    except Exception:
                        print "got exception in convert handler for '{0}'".format(key)
                        raise
                else:
                    self.data[key] = data[key]
                del data[key]
        if data:
            raise ValueError("passed data has unrecognized key '{0}'".format(data.keys()[0]))

    def get_data(self):
        """Returns a dictionary containing all the data needed to
        reconstruct this job on another machine, to be read in by
        `set_data`. If you want to change the internal representation
        used by your job, you should override this method and its twin
        `set_data`.
        """
        return self.data

    @classmethod
    def setup(cls):
        """This method should be called once, ever, for each Job type
        you will end up using. It creates storage on the backend. If
        you want to undo this, for whatever reason, use `teardown()`.
        """
        queue_name = cls.job_type_prefix + cls.job_type
        queue = boto.connect_sqs().create_queue(queue_name)
        domain = boto.connect_sdb().create_domain(queue_name)
        if not queue:
            raise RuntimeError("could not create SQS queue '{0}'".format(queue_name))
        if not domain:
            raise RuntimeError("could not create SDB domain '{0}'".format(queue_name))

    @classmethod
    def teardown(cls):
        """This method undoes what `setup()` does."""
        cls._open_queues()
        cls.job_queue.delete()
        cls.job_database.delete()

    @classmethod
    def _open_queues(cls):
        if cls.job_queue is None:
            queue_name = cls.job_type_prefix + cls.job_type
            cls.job_queue = boto.connect_sqs().get_queue(queue_name)
            cls.job_database = boto.connect_sdb().get_domain(queue_name)
            if not cls.job_queue:
                raise RuntimeError("could not open SQS queue '{0}'".format(queue_name))
            if not cls.job_database:
                raise RuntimeError("could not open SDB domain '{0}'".format(queue_name))

    @classmethod
    def fetch_next(cls, timeout=None):
        """This class method will return the next pending job of this
        type. Jobs returned this way can be `finish()`d and
        `error(...)`d. This is how workers get jobs to work on.

        Jobs returned this way will be hidden from other workers for a
        short amount of time. If you need more time before the job is
        done, use `update()`.
        """
        cls._open_queues()
        message = cls.job_queue.read(wait_time_seconds=timeout)
        if message is None:
            return None

        uuid = message.get_body()

        data = cls.job_database.get_item(uuid)
        if not data:
            # there is occasionally a race condition here
            # where the message is added before the SDB bit is updated
            # so, try again:
            time.sleep(1)
            data = cls.job_database.get_item(uuid)
        if not data:
            raise RuntimeError("job '{0}' does not have an SDB entry in '{1}'".format(uuid, cls.job_database.name))

        job = cls(uuid, message, data)
        job.status = INPROGRESS
        job.update()
        return job

    @classmethod
    def fetch_all(cls):
        """This method returns an iterator over *all* jobs of this
        type, including completed jobs.
        """
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}`".format(cls.job_database.name))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def fetch_all_pending(cls):
        """This method returns an iterator over all jobs that have not
        yet completed or error'd.
        """
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}` where status=\"{1}\" or status=\"{2}\"".format(cls.job_database.name, SUBMITTED, INPROGRESS))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def fetch_all_completed(cls):
        """This method returns an iterator over all jobs that have
        been completed or error'd.
        """
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}` where status=\"{1}\" or status=\"{2}\"".format(cls.job_database.name, COMPLETE, ERROR))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def submit(cls, **data):
        """This method submits a new job, and returns it. It accepts
        keyword arguments corresponding to job field names.
        """
        cls._open_queues()
        job = cls(gen_uuid().hex, None, data)
        message = cls.job_queue.new_message(body=job.uuid)
        job.message = message
        success = cls.job_database.put_attributes(job.uuid, job.get_data())
        if not success:
            raise RuntimeError("could not put key '{0}' in SDB domain '{1}'".format(job.uuid, cls.job_database.name))
        message = cls.job_queue.write(message)
        if not message:
            raise RuntimeError("could not write message '{0}' to SQS queue '{1}'".format(job.uuid, cls.job_queue.name))
        return job


class TestJob(Job):
    job_type = "test"
    job_fields = [
        "test",
        job_field("testdefault", 42, int),
    ]

import sys
if __name__ == "__main__":
    if '-setup' in sys.argv:
        TestJob.setup()
    if '-client' in sys.argv:
        while 1:
            j = TestJob.fetch_next(timeout=20)
            if j is None:
                continue

            print "got job", j.test, j.testdefault
            time.sleep(5)
            if int(j.test) % 2 == 0:
                j.error("number was even!")
            else:
                j.finish()
    if '-server' in sys.argv:
        for i in xrange(4):
            TestJob.submit(test=i)
            time.sleep(1)
    if '-monitor' in sys.argv:
        while 1:
            for job in TestJob.fetch_all():
                print job
            print "\n --- \n"
            time.sleep(1)
    if '-clean' in sys.argv:
        for job in TestJob.fetch_all_completed():
            job.delete()
