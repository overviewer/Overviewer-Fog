import time

from argparse import ArgumentParser
from jobs import WorldGenJob, RenderJob
from jobs import COMPLETE, ERROR


class ClientSubmitRenderCommand(object):
    name = "render"
    description = "Submit a job to render"

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
        parser.add_argument("url", type=str,
                            help="URL of a tarball'd Minecraft world")
        parser.add_argument("--monitor", action='store_true', default=False,
                            help="Wait until this job is complete")
        parser.add_argument("--rendermode", type=str, default="normal",
                            help="Rendermode to use")

        return parser

    def execute(self, args):

        world_url = args.url
        render_opts = dict()
        rendermode = args.rendermode
        render_opts['rendermode'] = rendermode
        print "Submitting job", world_url
        job = RenderJob.submit(world_url=world_url, render_opts=render_opts)
        print "Ok, job enqueued, job UUID:", job.uuid
        
        if args.monitor:
            print "Waiting until this job is complete.  It is safe to ctrl+c if you are tired of waiting"
            while (1):
                time.sleep(30)
                job.update_data()
                print "Status: %s\tprogress: %f" % (job.status, job.render_progress)
                if job.status == COMPLETE:
                    print "Your world is now available for viewing here:"
                    print job.rendered_url
                    break
                if job.status == ERROR:
                    print "Your job failed with the following message:"
                    print job.errmsg
                    break


class ClientGenWorldStatusCommand(object):
    name = "genworld-status"
    description = "Checks on the status of an already submitted job"

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
        parser.add_argument("uuid", type=str,
                            help="UUID of an already running job")
        return parser

    def execute(self, args):
        job = WorldGenJob.fetch_by_uuid(args.uuid)
        while (1):
            print "Status: %s" % job.status
            if job.status == COMPLETE:
                print "Your world is now available for download here:"
                print job.url
                break
            time.sleep(30)
            job.update_data()


class ClientGenWorldCommand(object):
    name = "genworld"
    description = "Generate a world from a seed string"

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
        parser.add_argument("--monitor", action='store_true', default=False,
                            help="Wait until this job is complete")
        parser.add_argument("seed", type=str,
                            help="Seed to use")
        parser.add_argument("spawn", nargs="?", type=str,
                            help="Optional spawn coords")
        return parser

    def execute(self, args):
        seed = args.seed
        if args.spawn:
            spawn = map(int, args.spawn.split(","))
            if len(spawn) != 3:
                raise ValueError("Bad spawn coord")
        else:
            spawn = []

        job = WorldGenJob.submit(seed=seed, spawn=spawn)

        print "Ok, job enqueued, job UUID:", job.uuid

        if args.monitor:
            print "Waiting until this job is complete.  It is safe to ctrl+c if you are tired of waiting"
            while (1):
                print "Status: %s" % job.status
                if job.status == COMPLETE:
                    print "Your world is now available for download here:"
                    print job.url
                    break
                time.sleep(30)
                job.update_data()
