#!/usr/bin/python2

import boto
import random
import time
import sys
import os

sys.path.append(".")
from jobs import Job, job_field

# NB: Don't begin the name of this wtih 'Test'.  pytest won't like that
class JobTestThing(Job):
    job_type = "test"
    job_fields = Job.job_fields + \
            [job_field("foo", 0, int),
             job_field("bar", "hi", str)]


class TestJobObject(object):
    def setup_class(cls):
        cls.sdb_conn = boto.connect_sdb()
        cls.sqs_conn = boto.connect_sqs()
        aws_suffix = os.environ.get("FOG_TEST_SUFFIX")
        cls.clean_on_teardown = False
        if aws_suffix is None:
            aws_suffix = "".join(map(chr,random.sample(range(ord('a'), ord('z')),9)))
            cls.clean_on_teardown = True
        cls.aws_suffix = aws_suffix

        try:
            db = cls.sdb_conn.get_domain("fogtestdb-" + aws_suffix)
        except Exception:
            db = cls.sdb_conn.create_domain("fogtestdb-" + aws_suffix)
            print "Creating new db domain:", aws_suffix
        JobTestThing.job_database = db

        q = cls.sqs_conn.get_queue("fogtestq-" + aws_suffix)
        if q is None:
            q = cls.sqs_conn.create_queue("fogtestq-" + aws_suffix)
            print "Creating new sqs queue:", aws_suffix
        JobTestThing.job_queue = q
   
    
    def teardown_class(cls):
        if cls.clean_on_teardown:
            JobTestThing.job_database.delete()
            JobTestThing.job_queue.delete()

    def test1(self):
        # Submit a job"
        r = random.randint(5,500)
        JobTestThing.submit(foo=r)
        time.sleep(1)

        # try for at more 15 seconds to get a job
        for x in range(15):
            j = JobTestThing.fetch_next()
            if j is not None: break
            time.sleep(1)
        assert j is not None

        assert j.foo == r
        assert j.bar == "hi"

        

