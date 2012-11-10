#!/usr/bin/python2

from fog.cli import run_worker, WorkerCommand
from fog.worker_commands import WorkerGenWorldCommand

if __name__ == "__main__":
    run_worker([WorkerCommand, WorkerGenWorldCommand])
