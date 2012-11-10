#!/usr/bin/python2

from fog.cli import run_client, ClientCommand
from fog.client_commands import ClientGenWorldCommand

if __name__ == "__main__":
    run_client([ClientCommand, ClientGenWorldCommand])
