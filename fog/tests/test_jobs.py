#!/usr/bin/python2

import random
import time
import os

from ..jobs import *


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
        uuid = JobTestThing.submit(foo=r).uuid
        time.sleep(1)

        # try for at most 15 seconds to get a job
        j = JobTestThing.fetch_next(timeout=15)
        assert j is not None

        assert j.foo == r
        assert j.bar == "hi"
        # wait for status to flip to INPROGRESS
        for c in range(15):
            if JobTestThing.fetch_by_uuid(uuid).status != INPROGRESS:
                time.sleep(1)
            else:
                break
        else:
            assert JobTestThing.fetch_by_uuid(uuid).status == INPROGRESS
        j.finish()

        # wait for status to flip to COMPLETE
        assert JobTestThing.fetch_by_uuid(uuid).status == COMPLETE
        for c in range(15):
            if JobTestThing.fetch_by_uuid(uuid).status != COMPLETE:
                time.sleep(1)
            else:
                break
        else:
            assert JobTestThing.fetch_by_uuid(uuid).status == COMPLETE
