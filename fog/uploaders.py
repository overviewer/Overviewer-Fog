

"""

TODO put some docs here


"""
import os
import subprocess
import shutil
import tempfile


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
    defaults = dict(username="upload")

    def __init__(self, username=None, key=None):
        self.dict = dict(username=username, privkey=key)

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

    def upload_dir_as_file(self, localdir, remotename, gzip=True):
        "Uploads the entire contents of a directory as an possibly gzip compressed tarball"

        k = self.boto.s3.key.Key(self.bucket)
        k.key = remotename

        tarargs = ["tar", "-c"]
        args = []
        if gzip:
            tarargs.append("-z")
            args.append("-gzip")

        tarargs += ["-C", localdir, "."]

        # we cannot stream data into S3, so we must save the tarball locally first
        tarball = tempfile.NamedTemporaryFile(delete=False)

        tarp = subprocess.Popen(tarargs, stdout=tarball)
        tarp.wait()
        tarball.close()

        k.set_contents_from_filename(tarball.name, reduced_redundancy=True)

        k.make_public()
        url = self.urlbase + k.key

        os.unlink(tarball.name)

        return url

    def delete_file(self, remotename):
        k = self.bucket.get_key(remotename)
        k.delete()


class OVUploader(Uploader):
    """An anonymous ssh-based uploaded to paphlagon"""
    def __init__(self, hostname, credentials):
        self.cred = credentials
        self.host = hostname

    def upload_file(self, file, gzip=False):
        """Uploads a single file.  Set gzip to True to incidate that the local
        file should be gzip compressed, and that it should be uncompressed on the remote
        system"""
        args = []
        if gzip:
            args.append("-gzip")

        p = subprocess.Popen(["ssh", "-T",
                             "-l", self.cred.get("username"),
                             "-i", self.cred.get("privkey"),
                             self.host,
                             "do_upload",
                             "-file"] + args,
                             stdin=subprocess.PIPE)
        pipe_obj = p.stdin

        if gzip:
            gzip_p = subprocess.Popen(["gzip", "-c"], stdin=subprocess.PIPE, stdout=p.stdin)
            pipe_obj = gzip_p.stdin

        with open(file) as fobj:
            shutil.copyfileobj(fobj, pipe_obj)
        pipe_obj.close()
        if gzip:
            gzip_p.wait()
            p.stdin.close()
        p.wait()

    def upload_dir(self, directory, elements=['.'], gzip=True):
        """Uploads the entire contents of a directory.  The files will be streamed with tar,
        and compressed with gzip.  Set gzip to false to disable compression"""

        tarargs = ["tar", "-c"]
        args = []
        if gzip:
            tarargs.append("-z")
            args.append("-gzip")

        tarargs += ["-C", directory, ] + elements

        p = subprocess.Popen(["ssh", "-T",
                             "-l", self.cred.get("username"),
                             "-i", self.cred.get("privkey"),
                             self.host,
                             "do_upload",
                             "-dir"] + args,
                             stdin=subprocess.PIPE)

        tarp = subprocess.Popen(tarargs, stdout=p.stdin, cwd=directory)
        tarp.wait

        p.stdin.close()
        p.wait()


class RsyncUploader(Uploader):
    """An authenticatd rsync-based uploaded"""
    pass
