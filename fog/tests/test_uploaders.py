
import os
import random
import tempfile
import urllib
import pytest
import subprocess
from getpass import getuser

from ..uploaders import *


def test_awscreds():
    cred = AWSCredentials.build_from_environment()
    assert cred.get("key") == os.environ.get("AWS_ACCESS_KEY_ID")
    assert cred.get("secret") == os.environ.get("AWS_SECRET_ACCESS_KEY")


def gen_testfile():
    # generate some random data to upload
    randdata = "".join([chr(random.randint(0, 255)) for _ in range(1024 * 10)])
    tmpfil = tempfile.NamedTemporaryFile(delete=False)
    tmpfil.write(randdata)
    tmpfil.close()

    return tmpfil, randdata


class TestAWSUploader(object):
    def test_uploadfile(self):
        s3 = S3Uploader()

        tmpfil, randdata = gen_testfile()

        url = s3.upload_file(tmpfil.name)

        s3data = urllib.urlopen(url).read(1024 * 10)
        assert randdata == s3data

        s3.delete_file(os.path.basename(tmpfil.name))

        os.unlink(tmpfil.name)


@pytest.mark.skipif("os.environ.get('FOG_TEST_OVUPLOADER','') == ''")
class TestOVUploader(object):
    """These tests will generate sshkeys, modify your .ssh/authorized_keys file,
    and require working ssh.  Since these things may cause problems, it's not expected
    that this test will want to be run by default.  Therefore, set the FOG_TEST_OVUPLOADER
    environment variable if you want to run these tests
    """
    def setup_class(cls):
        # create a new ssh keypair for this test
        cls.tmpdir = tempfile.mkdtemp()
        cls.keyname = os.path.join(cls.tmpdir, "key")
        p = subprocess.Popen(["ssh-keygen", "-b", "1024", "-f", cls.keyname, "-P", "", "-C", "Pytest test key"])
        p.wait()
        assert p.returncode == 0

        # save a copy of our authorized_keys file
        cls.authkeys = os.path.expanduser("~/.ssh/authorized_keys")
        if os.path.exists(cls.authkeys):
            cls.authkeys_back = os.path.expanduser("~/.ssh/authorized_keys.fogback")
            shutil.copy2(cls.authkeys, cls.authkeys_back)
        else:
            cls.authkeys_back = None

        # stick our test key into authorized_keys
        do_upload = os.path.abspath(os.path.join("scripts", "do_upload.py"))
        assert os.path.exists(do_upload)

        with open(cls.authkeys, "a+") as f:
            with open(cls.keyname + ".pub") as keyf:
                f.write('''command="/opt/local/bin/python2 %s",no-port-forwarding,no-pty ''' % do_upload)
                f.write(keyf.read())

    def teardown_class(cls):

        # restore our authorized_keys file from backup
        if cls.authkeys_back is None:
            unlink(cls.authkeys)
        else:
            shutil.copy2(cls.authkeys_back, cls.authkeys)

        shutil.rmtree(cls.tmpdir)

    def test_uploadfile(self):
        creds = SSHCredentials(getuser(), TestOVUploader.keyname)
        ov = OVUploader("localhost", creds)

        tmpfil, randdata = gen_testfile()
        ov.upload_file(tmpfil.name)

        with open("/tmp/incoming.dat", "rb") as f:
            assert f.read(len(randdata)) == randdata
        os.unlink(tmpfil.name)

    def test_uploadfile_bzip(self):
        creds = SSHCredentials(getuser(), TestOVUploader.keyname)
        ov = OVUploader("localhost", creds)

        tmpfil, randdata = gen_testfile()
        ov.upload_file(tmpfil.name, True)

        with open("/tmp/incoming.dat", "rb") as f:
            assert f.read(len(randdata)) == randdata
        os.unlink(tmpfil.name)
