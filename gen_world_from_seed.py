#!/usr/bin/python2

import sys
import os
import tempfile
import subprocess
import shutil

from jobs import *


class WorldGenJob(Job):
    job_type = "worldgen"
    job_fields = [
        job_field("seed", '', str),
        job_field("spawn", [], list)
    ]

try:
    import redstone  # <3 libredstone
except ImportError:
    sys.path.append(os.path.expanduser("~/devel/libredstone/bindings"))
    import redstone


from boto.s3.connection import S3Connection
from boto.s3.key import Key

# provide your own config.py that holds useful
# configuration settings
import config


if "AWS_ACCESS_KEY_ID" not in os.environ:
    print "Please set the AWS_ACCESS_KEY_ID environment variable"
if "AWS_SECRET_ACCESS_KEY" not in os.environ:
    print "Please set the AWS_SECRET_ACCESS_KEY environment variable"


def submit():
    if len(sys.argv) < 3:
        print "Usage:"
        print "  %s -seed <seed> [<spawn coords>]" % sys.argv[0]
        return

    # TODO use less crappy command line parsing
    seed = sys.argv[2]

    if len(sys.argv) == 4:
        spawn = [int(x) for x in sys.argv[3].split(",")]
        assert(len(spawn) == 3)
        print "Generate world with this seed (\"%s\") with spawn %r [y/N]?" % (seed, spawn)
    else:
        spawn = []
        print "Generate world with this seed (\"%s\") [y/N]?" % seed
    if raw_input().lower() == 'y':
        job = WorldGenJob.submit(seed=seed, spawn=spawn)

        print "Ok, job enqueued"
        print "job UUID:", job.uuid

    else:
        print "Ok, not submitting.  Bye"
        return


def generate():

    job = WorldGenJob.fetch_next(15)
    if job is None:
        print "Nothing in the queue.  Please try again later"
        return 0

    seed = job.seed
    spawn = job.spawn

    print "got job"
    print "seed", seed
    print "spawn", spawn

    # check config options
    if not os.path.isfile(config.minecraft_server):
        raise Exception("minecraft_server isn't configured")

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

    job.update(5 * 60)
    print "Making tarball..."
    p = subprocess.Popen(["tar", "-cf", "world.tar", "world/"],
                         cwd=tmpdir)
    p.wait()
    if p.returncode != 0:
        print "***Error: tar failed"
        return
    print "OK."
    print "Compressing..."
    p = subprocess.Popen(["bzip2", "world.tar"],
                         shell=False,
                         cwd=tmpdir)
    p.wait()
    if p.returncode != 0:
        print "***Error: compress failed"
        return
    print "OK."

    s3 = S3Connection()
    bucket = s3.get_bucket("overviewer-worlds")
    k = Key(bucket)
    k.key = "%s.tar.bz2" % job.uuid
    print "Uploading to S3..."
    k.set_contents_from_filename(os.path.join(tmpdir, "world.tar.bz2"), reduced_redundancy=True)
    print "OK."
    k.make_public()

    urlbase = "https://s3.amazonaws.com/overviewer-worlds/"
    url = urlbase + k.key
    print "World is now available at:", url

    job.finish()

    print "All done!"

if __name__ == "__main__":
    if "-seed" in sys.argv:
        submit()
    elif "-generate" in sys.argv:
        generate()
    else:
        print "Usage:"
        print "  %s -seed <seed> [<spawn coords>]" % sys.argv[0]
        print "  %s -generate" % sys.argv[0]
