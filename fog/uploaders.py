

"""

TODO put some docs here


"""
import os
import subprocess
import shutil


class Uploader(object):
    def upload_file(self, file):
        raise NotImplementedError()

    def upload_directory(self, dir):
        raise NotImplementedError()


class Credentials(object):
    def get(self, key):
        return self.dict[key]


class AWSCredentials(Credentials):

    def __init__(self, key, secret):
        self.dict = dict(key=key, secret=secret)

    def get(self, key):
        return self.dict[key]

    @classmethod
    def build_from_environment(cls):
        return cls(os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY"))


class SSHCredentials(Credentials):
    defaults = dict(username="upload", host="overviewer.org")

    def __init__(self, username=None, key=None, host=None):
        self.dict = dict(username=username, privkey=key, host=host)

    def get(self, key):
        return self.dict.get(key, SSHCredentials.defaults.get(key))


class S3Uploader(Uploader):
    """An S3-based uploader"""
    def __init__(self, credentials=None):
        import boto
        self.boto = boto

        aws_access_key_id = None
        aws_secret_access_key = None
        if credentials is not None:
            aws_access_key_id = credentials.get("key")
            aws_secret_access_key = credentials.get("secret")

        self.s3 = boto.connect_s3(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
        self.bucket = self.s3.get_bucket("overviewer-worlds")
        self.urlbase = "https://s3.amazonaws.com/%s/" % self.bucket.name

    def upload_file(self, localfile, remotename=None):
        k = self.boto.s3.key.Key(self.bucket)
        if remotename is not None:
            k.key = remotename
        else:
            k.key = os.path.basename(localfile)
        k.set_contents_from_filename(localfile, reduced_redundancy=True)
        k.make_public()
        url = self.urlbase + k.key
        return url

    def delete_file(self, remotename):
        k = self.bucket.get_key(remotename)
        k.delete()


class OVUploader(Uploader):
    """An anonymous ssh-based uploaded to paphlagon"""
    def __init__(self, credentials):
        self.cred = credentials

    def upload_file(self, file):
        p = subprocess.Popen(["ssh",
                             "-l", self.cred.get("username"),
                             "-i", self.cred.get("privkey"),
                             "new.overviewer.org",
                             render_uuid],
                             stdin=subprocess.PIPE)
        with open(file) as fobj:
            shutil.copyfileobj(fobj, p.stdin)
        p.stdin.close()
        p.wait()


class RsyncUploader(Uploader):
    """An authenticatd rsync-based uploaded"""
    pass
