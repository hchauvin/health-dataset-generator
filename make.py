#!/usr/bin/env python3

# This script requires python 3.7 or higher.

import argparse
import os
import shutil
import sys
from pathlib import Path
from subprocess import run

os.environ["DOCKER_BUILDKIT"] = "1"

DOCKER_IMAGE = "hchauvin/eds-generator-notebooks"
CONTAINER_NAME = "eds-generator-notebooks"


def docker_run(build, volumes):
    if build:
        docker_build()
    cwd = os.getcwd()
    run(["docker", "rm", "-f", CONTAINER_NAME], check=False,
        capture_output=True)

    args = ["docker", "run",
            "--name", CONTAINER_NAME,
            "-v", f"{cwd}/notebooks:/opt/generator/notebooks",
            ]
    for v in volumes:
        args += ["-v", v]
    args += [
        "-p", "127.0.0.1:8192:8192", "-p", "127.0.0.1:4040-4050:4040-4050",
        DOCKER_IMAGE]
    run(args, check=True)


def docker_build():
    ensure_third_party()
    run(["docker", "build", "-t", DOCKER_IMAGE, "."], check=True)


def docker_push():
    run(["docker", "push", DOCKER_IMAGE], check=True)


def dev_third_party():
    cwd = os.getcwd()
    run(["docker", "build", "-t", "eds-generator-third-party",
         "docker/third-party"], check=True)
    run(["docker", "rm", "-f", "eds-generator-third-party"], check=False,
        capture_output=True)
    run(["docker", "create", "--name", "eds-generator-third-party",
         "eds-generator-third-party"], check=True)
    if Path("third-party").exists():
        shutil.rmtree("third-party")
    run(["docker", "cp", "eds-generator-third-party:/opt/generator/third-party",
         f"{cwd}/third-party"], check=True)
    run(["docker", "rm", "eds-generator-third-party"], check=True)


def dev_test():
    run(["mvn", "test"], check=True)


def dev_format():
    run(["mvn", "validate"], check=True)


def dev_format_check():
    run(["mvn", "validate", "-Dformat.check=true"], check=True)


def ensure_third_party():
    if not Path("third-party").exists():
        dev_third_party()


class Make:
    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Make',
            usage="make <command> [<args>]")
        subcommands = [attr for attr in dir(self) if not attr.startswith("__")]
        parser.add_argument('command',
                            help='Subcommand to run: one of ' + " ".join(subcommands))
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def docker_run(self):
        parser = argparse.ArgumentParser(
            description='Run the docker image')
        parser.add_argument('-b', '--build', dest='build', action='store_true', help='build the image beforehand')
        parser.add_argument('-v', '--volume', nargs='*',
                            help='A volume to mount')
        args = parser.parse_args(sys.argv[2:])
        docker_run(build=args.build, volumes=args.volume)

    def docker_build(self):
        parser = argparse.ArgumentParser(
            description='Build the docker image')
        args = parser.parse_args(sys.argv[2:])
        docker_build()

    def docker_push(self):
        parser = argparse.ArgumentParser(
            description='Push the docker image')
        args = parser.parse_args(sys.argv[2:])
        docker_push()

    def dev_third_party(self):
        parser = argparse.ArgumentParser(
            description="Download/compile third-party dependencies and put them in the 'third-party' directory")
        args = parser.parse_args(sys.argv[2:])
        dev_third_party()

    def dev_test(self):
        parser = argparse.ArgumentParser(
            description="Execute tests")
        args = parser.parse_args(sys.argv[2:])
        dev_test()

    def dev_format(self):
        parser = argparse.ArgumentParser(
            description='Format source files')
        args = parser.parse_args(sys.argv[2:])
        dev_format()

    def dev_format_check(self):
        parser = argparse.ArgumentParser(
            description='Check source file formatting')
        args = parser.parse_args(sys.argv[2:])
        dev_format_check()


if __name__ == "__main__":
    try:
        Make()
    except KeyboardInterrupt as e:
        print("Interrupted")
        sys.exit(1)
