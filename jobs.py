#!/usr/bin/python2

import boto
from uuid import uuid4 as gen_uuid
import time


# various status codes
SUBMITTED = "submitted"
INPROGRESS = "inprogress"
COMPLETE = "complete"
ERROR = "error"


def job_field(name, default=None, convert=None):
    if default is not None and not callable(default):
        get_default = lambda: default
    else:
        get_default = default
    return (name, get_default, convert)


class Job(object):
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
        self.uuid = uuid
        self.message = message
        self.set_data(data)

    def __repr__(self):
        return "<{0} {1}>".format(self.__class__.__name__, repr(self.data))

    def __getattr__(self, name):
        # shim for before data is set
        if name == 'data':
            return {}

        try:
            return self.data[name]
        except KeyError:
            raise AttributeError(name)

    def __setattr__(self, name, value):
        if not name in self.data:
            super(Job, self).__setattr__(name, value)
        else:
            self.data[name] = value

    def update(self, visibility_timeout=60):
        if self.message:
            self.message.change_visibility(visibility_timeout=visibility_timeout)
        self._open_queues()
        self.job_database.put_attributes(self.uuid, self.get_data())

    def finish(self):
        assert self.message
        self.message.delete()
        self.message = None

        self.status = COMPLETE
        self.completed = time.time()
        self.update()

    def error(self, message):
        assert self.message
        self.message.delete()
        self.message = None

        self.status = ERROR
        self.completed = time.time()
        self.errmsg = message
        self.update()

    def delete(self):
        if self.message:
            self.message.delete()
            self.message = None
        self._open_queues()
        self.job_database.delete_attributes(self.uuid)

    def set_data(self, data):
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
                    self.data[key] = get_default()
            else:
                if convert:
                    self.data[key] = convert(data[key])
                else:
                    self.data[key] = data[key]
                del data[key]
        if data:
            raise ValueError("passed data has unrecognized key '{0}'".format(data.keys()[0]))

    def get_data(self):
        return self.data

    @classmethod
    def setup(cls):
        queue_name = cls.job_type_prefix + cls.job_type
        queue = boto.connect_sqs().create_queue(queue_name)
        domain = boto.connect_sdb().create_domain(queue_name)
        if not queue:
            raise RuntimeError("could not create SQS queue '{0}'".format(queue_name))
        if not domain:
            raise RuntimeError("could not create SDB domain '{0}'".format(queue_name))

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
        assert data

        job = cls(uuid, message, data)
        job.status = INPROGRESS
        job.update()
        return job

    @classmethod
    def fetch_all(cls):
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}`".format(cls.job_database.name))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def fetch_all_pending(cls):
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}` where status=\"{1}\" or status=\"{2}\"".format(cls.job_database.name, SUBMITTED, INPROGRESS))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def fetch_all_completed(cls):
        cls._open_queues()
        rs = cls.job_database.select("select * from `{0}` where status=\"{1}\" or status=\"{2}\"".format(cls.job_database.name, COMPLETE, ERROR))
        for j in rs:
            yield cls(j.name, None, dict(j))

    @classmethod
    def submit(cls, **data):
        cls._open_queues()
        job = cls(gen_uuid().hex, None, data)
        message = cls.job_queue.new_message(body=job.uuid)
        job.message = message
        cls.job_database.put_attributes(job.uuid, job.get_data())
        cls.job_queue.write(message)
        return job


class TestJob(Job):
    job_type = "test"
    job_fields = ["test", job_field("testdefault", 42, int)]

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
