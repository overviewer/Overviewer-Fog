from argparse import ArgumentParser
import sys


class ClientCommand(object):
    name = "dummy"
    description = "a dummy command"

    @classmethod
    def get_argument_parser(cls):
        return ArgumentParser()

    def execute(self, args):
        print "dummy", args


class WorkerCommand(object):
    name = "dummy"
    description = "a dummy command"

    @classmethod
    def get_argument_parser(cls):
        return ArgumentParser()

    def execute(self, config, args):
        print "dummy", config, args


def run(commands, client=True):
    cmdmap = {}
    for cmd in commands:
        cmdmap[cmd.name] = cmd

    if len(sys.argv) == 1 or not sys.argv[1].lower() in cmdmap:
        if len(sys.argv) != 1:
            print "invalid command:", sys.argv[1].lower()
        if client:
            print "usage: {0} <command> [.. options ..]".format(sys.argv[0])
        else:
            print "usage: {0} <command> [--config=config.py] [.. options ..]".format(sys.argv[0])
        print
        print "<command> is one of:"
        for cmd in commands:
            print "  {0} - {1}".format(cmd.name, cmd.description)
        sys.exit(1)

    cmd = cmdmap[sys.argv[1].lower()]
    sys.argv[0] = sys.argv[0] + " " + sys.argv[1]
    del sys.argv[1]

    parser = cmd.get_argument_parser()
    if not client:
        parser.add_argument("--config", dest="config", default=None, type=file)

    args = parser.parse_args()

    cmdobj = cmd()
    if client:
        cmdobj.execute(args)
    else:
        if args.config:
            configglobals = {}
            config = {}
            exec args.config in configglobals, config
        else:
            config = {}
        cmdobj.execute(config, args)


def run_client(commands):
    return run(commands, client=True)


def run_worker(commands):
    return run(commands, client=False)
