import sys
import os
import tempfile
import subprocess
import shutil

try:
    import redstone  # <3 libredstone
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/libredstone/bindings"))
    import redstone

from argparse import ArgumentParser
from jobs import WorldGenJob, RenderJob
from uploaders import S3Uploader, SSHCredentials, OVUploader
import config
import urllib


class WorkerDoRenderCommand(object):
    name = "dorender"
    description = "Fetches and runs 1 RenderJob job"

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
        return parser

    def execute(self, cfgfile, args):
        # check config options
        if not os.path.isdir(config.overviewer_directory):
            raise Exception("overviewer_directory isn't configured")

        job = RenderJob.fetch_next(15)
        if job is None:
            print "No jobs"
            return

        print "Working on job", job.uuid
        url = job.world_url
        render_opts = job.render_opts

        tmpdir = tempfile.mkdtemp(prefix="fog_render")

        print "Getting map..."
        map_url = urllib.urlopen(url)
        print "OK."

        fobj = open(os.path.join(tmpdir, "world.tgz"), "w")
        print "Downloading map to %s..." % tmpdir
        shutil.copyfileobj(map_url, fobj)
        fobj.close()
        print "OK."

        print "Uncompressing..."
        os.mkdir(os.path.join(tmpdir, "world"))
        p = subprocess.Popen(["tar", "-zxf", os.path.join(tmpdir, "world.tgz")],
                             cwd=os.path.join(tmpdir, "world"))
        p.wait()
        if p.returncode != 0:
            print "***Error: decompressing"
            return 1

        # find the exact directory containing level.dat
        def findLevel(start):
            for root, dirs, files in os.walk(start):
                if "level.dat" in files:
                    return root
                for d in dirs:
                    l = findLevel(os.path.join(root, d))
                    if l is not None:
                        return l

        real_world_dir = findLevel(os.path.join(tmpdir, "world"))
        if real_world_dir is None:
            raise Exception("Failed to find level.dat anywhere in %r" % os.path.join(tmpdir, "world"))

        print "Rendering..."
        p = subprocess.Popen([sys.executable,
                              os.path.join(config.overviewer_directory, "overviewer.py"),
                              real_world_dir,
                              os.path.join(tmpdir, "output_dir_" + job.uuid),
                              "--rendermode",
                              render_opts['rendermode']])
        p.wait()
        if p.returncode != 0:
            print "***Error: rendering"
            job.error("Failed to render")
            return 1
        print "Ok."

        job.rendered_url = os.path.join(tmpdir, "output_dir")

        creds = SSHCredentials(config.ov_upload_username, config.ov_upload_key)
        ov = OVUploader(config.ov_upload_hostname, creds)

        ov.upload_dir(tmpdir, ["output_dir_" + job.uuid])

        job.finish()

        #shutil.rmtree(tmpdir)


class WorkerGenWorldCommand(object):
    name = "genworld"
    description = "Fetches and runs 1 GenWorld job"
    s3 = S3Uploader()

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
        return parser

    def execute(self, cfgfile, args):
        # check config options
        if not os.path.isfile(config.minecraft_server):
            raise Exception("minecraft_server isn't configured")

        job = WorldGenJob.fetch_next(15)
        if job is None:
            print "No jobs"
            return
        print "Working on job", job.uuid
        seed = job.seed
        spawn = job.spawn

        # with a generous amount of rounding up, assume it'll take 5 minutes to generate the map
        job.update(5 * 60)
        tmpdir = tempfile.mkdtemp(prefix="mc_gen")

        # create a settings.properties file with our seed
        with open(os.path.join(tmpdir, "server.properties"), "w") as f:
            f.write("level-seed=%s" % seed)

        p = subprocess.Popen(["java", "-jar",
                             config.minecraft_server, "nogui"],
                             shell=False,
                             stdin=subprocess.PIPE,
                             cwd=tmpdir)
        p.stdin.write("stop\n")
        p.stdin.close()
        p.wait()
        print ""
        print "Minecraft server exited with %r" % p.returncode
        print "World resided in %r" % tmpdir

        # if we want a specific spawn, we need to rewrite the level.dat file,
        # remove the old region files, and restart the server
        if len(spawn) > 0:
            leveldat = redstone.NBT.parse_from_file(os.path.join(tmpdir, "world", "level.dat"))
            root = leveldat.root
            root['Data']['SpawnX'].set_integer(int(spawn[0]))
            root['Data']['SpawnY'].set_integer(int(spawn[1]))
            root['Data']['SpawnZ'].set_integer(int(spawn[2]))
            leveldat.write_to_file(os.path.join(tmpdir, "world", "level.dat"))

            shutil.rmtree(os.path.join(tmpdir, "world", "region"))
            p = subprocess.Popen(["java", "-jar",
                                 config.minecraft_server, "nogui"],
                                 shell=False,
                                 stdin=subprocess.PIPE,
                                 cwd=tmpdir)

            p.stdin.write("stop\n")
            p.stdin.close()
            p.wait()
            print "Minecraft server exited with %r" % p.returncode

        remotefile = job.uuid + ".tgz"
        url = self.s3.upload_dir_as_file(tmpdir, remotefile, gzip=True)

        job.url = url

        job.finish()
        print "All done! World has been uploaded to:"
        print url
