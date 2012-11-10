from argparse import ArgumentParser
from jobs import WorldGenJob


class ClientGenWorldCommand(object):
    name = "genworld"
    description = "Generate a world from a seed string"

    @classmethod
    def get_argument_parser(cls):
        parser = ArgumentParser()
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

        print "Ok, job enqueued, UUID:", job.uuid
