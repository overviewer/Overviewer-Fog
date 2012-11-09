#!/usr/bin/python2

import random
import time
import sys
import os

sys.path.append(".")
from jobs import Job, job_field


# NB: Don't begin the name of this wtih 'Test'.  pytest won't like that
class JobTestThing(Job):
    job_type = "test"
    job_fields = [
        job_field("foo", 0, int),
        job_field("bar", "hi", str),
    ]


class TestJobObject(object):
    def setup_class(cls):
        aws_suffix = os.environ.get("FOG_TEST_SUFFIX")
        cls.clean_on_teardown = False
        if aws_suffix is None or aws_suffix == "":
            aws_suffix = "".join(map(chr, random.sample(range(ord('a'), ord('z')), 9)))
            cls.clean_on_teardown = True

        JobTestThing.job_type = aws_suffix
        JobTestThing.job_type_prefix = "fogtest-"

        JobTestThing.setup()

    def teardown_class(cls):
        if cls.clean_on_teardown:
            JobTestThing.teardown()

    def test1(self):
        # Submit a job
        r = random.randint(5, 500)
        JobTestThing.submit(foo=r)
        time.sleep(1)

        # try for at more 15 seconds to get a job
        j = JobTestThing.fetch_next(timeout=15)
        assert j is not None

        assert j.foo == r
        assert j.bar == "hi"
