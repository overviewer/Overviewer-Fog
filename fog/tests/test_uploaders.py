
import os
import random
import tempfile
import urllib

from ..uploaders import *


def test_awscreds():
    cred = AWSCredentials.build_from_environment()
    assert cred.get("key") == os.environ.get("AWS_ACCESS_KEY_ID")
    assert cred.get("secret") == os.environ.get("AWS_SECRET_ACCESS_KEY")


class TestAWSUploader(object):
    def test_uploadfile(self):
        s3 = S3Uploader()

        # generate some random data to upload
        randdata = "".join([chr(random.randint(0, 255)) for _ in range(1024 * 10)])
        tmpfil = tempfile.NamedTemporaryFile(delete=False)
        tmpfil.write(randdata)
        tmpfil.close()

        url = s3.upload_file(tmpfil.name)

        s3data = urllib.urlopen(url).read(1024 * 10)
        assert randdata == s3data

        s3.delete_file(os.path.basename(tmpfil.name))

        os.unlink(tmpfil.name)
