#!/usr/bin/env python3

# NOTE: This script requires python 3.7 or higher.

import argparse
import os
import re
import shutil
import sys
from pathlib import Path
from subprocess import run


def content_addressable_imgref(image_repository, image_id):
    """
    Creates a content-addressable image reference for an image with a given
    image ID to store in a given image repository: the resulting image
    reference will uniquely address the content of the image.

    Args:
        image_repository: The repository to use in the image reference.
        image_id: The ID of the image.
    """
    return image_repository + ':' + image_id[image_id.index(':') + 1:][:40]


class Docker:
    """
    All Docker-related commands.
    """

    DOCKER_IMAGE = "hchauvin/eds-generator-notebooks"
    CONTAINER_NAME = "eds-generator-notebooks"

    def __init__(self, with_buildkit, with_cache_from):
        """
        Args:
            with_buildkit: Whether to run with buildkit support (not available on
                some runtimes).
            with_cache_from: Whether to use the --cache-from option.
        """
        env = {**os.environ, "DOCKER_BUILDKIT": "1" if with_buildkit else "0"}

        if with_buildkit:
            dockerfile = "./Dockerfile"
        else:
            # We need to disable buildkit-specific features in the Dockerfile
            with open("Dockerfile", "r") as f:
                dockerfile = f.read()
            dockerfile = re.compile(r"\s*--mount=[^\s]+ \\\n",
                                    re.MULTILINE).sub("", dockerfile)
            with open("Dockerfile.nobuildkit", "w") as f:
                f.write(dockerfile)
            dockerfile = "./Dockerfile.nobuildkit"

        self.env = env
        self.dockerfile = dockerfile
        self.with_cache_from = with_cache_from

    def docker_run(self, build, volumes, extra_parameters):
        if build:
            self.docker_build()

        if os.path.exists('target/image_reference.txt'):
            with open('target/image_reference.txt', 'r') as f:
                imgref = f.read().strip()
        else:
            imgref = Docker.DOCKER_IMAGE

        run(["docker", "rm", "-f", imgref],
            check=False,
            capture_output=True,
            env=self.env)

        cwd = os.getcwd()

        args = [
            "docker",
            "run",
            "--name",
            Docker.CONTAINER_NAME,
            "-v",
            f"{cwd}/notebooks:/opt/generator/notebooks",
        ]
        for v in volumes:
            args += ["-v", v]
        args += [f'--{p}' for p in extra_parameters]
        args += [
            "-p", "127.0.0.1:8192:8192", "-p", "127.0.0.1:4040-4050:4040-4050",
            Docker.DOCKER_IMAGE
        ]

        run(args, check=True)

    def docker_build(self, tag=DOCKER_IMAGE, target=None):
        os.makedirs("target", exist_ok=True)

        args = [
            "docker", "build", "-f", self.dockerfile, "-t", tag, "--iidfile",
            "target/image_id.txt"
        ]
        if self.with_cache_from:
            args += [
                "--cache-from",
                Docker.DOCKER_IMAGE,
                "--cache-from",
                self.intermediate_docker_image("third-party"),
            ]
        if target:
            args += ["--target", target]
        args += ["."]
        run(args, check=True, env=self.env)

        with open('target/image_id.txt', 'r') as f:
            image_id = f.read().strip()
        imgref = content_addressable_imgref(Docker.DOCKER_IMAGE, image_id)
        run(['docker', 'tag', image_id, imgref], check=True, env=self.env)
        with open('target/image_reference.txt', 'w') as f:
            f.write(imgref)

    def docker_push(self):
        intermediate_docker_images = ["synthea", "third-party"]
        for image in intermediate_docker_images:
            self.docker_build(
                tag=self.intermediate_docker_image(image), target=image)
            run(["docker", "push",
                 self.intermediate_docker_image(image)],
                check=True,
                env=self.env)

        self.docker_build()
        run(["docker", "push", Docker.DOCKER_IMAGE], check=True, env=self.env)

        with open('target/image_reference.txt', 'r') as f:
            imgref = f.read().strip()
        run(["docker", "tag", Docker.DOCKER_IMAGE, imgref],
            check=True,
            env=self.env)
        run(["docker", "push", imgref], check=True, env=self.env)

    def intermediate_docker_image(self, name):
        return f"{Docker.DOCKER_IMAGE}-{name}"

    def intermediate_container_name(self, name):
        return f"{Docker.CONTAINER_NAME}-{name}"


class Dev:
    """
    All the commands related to local development.

    Args:
        docker: A Docker object.
    """

    def __init__(self, docker):
        self.docker = docker

    def third_party(self):
        third_party_docker_image = self.docker.intermediate_docker_image(
            "third-party")
        third_party_container_name = self.docker.intermediate_container_name(
            "third-party")

        cwd = os.getcwd()
        self.docker.docker_build(
            tag=third_party_docker_image, target="third-party")
        run(["docker", "rm", "-f", third_party_container_name],
            check=False,
            capture_output=True)
        run([
            "docker", "create", "--name", third_party_container_name,
            third_party_docker_image
        ],
            check=True)
        if Path("third-party").exists():
            shutil.rmtree("third-party")
        run([
            "docker", "cp",
            f"{third_party_container_name}:/opt/generator/third-party",
            f"{cwd}/third-party"
        ],
            check=True)
        run(["docker", "rm", third_party_container_name], check=True)

        run([
            "mvn", "install:install-file",
            f"-Dfile={cwd}/third-party/synthea-with-dependencies.jar",
            "-DgroupId=org.mitre.synthea", "-DartifactId=synthea",
            "-Dversion=2.6.0-SNAPSHOT", f"-DlocalRepositoryPath={cwd}/repo",
            "-Dpackaging=jar"
        ],
            check=True)

    def package(self):
        self._ensure_third_party()
        run([
            "mvn", "package", "-T 1.5C", "-Dmaven.test.skip=true",
            "-DskipTests", "-P", "generator-target-eds-fat-jar"
        ],
            check=True)

    def test(self):
        self._ensure_third_party()
        run(["mvn", "test"], check=True)

    def format(self):
        self._ensure_third_party()
        run([
            "mvn", "validate", "-T 1.5C", "-Dformat.check=false",
            "-Dformat.skipTestSources=false", "-Dformat.skipSources=false"
        ],
            check=True)

    def format_check(self):
        self._ensure_third_party()
        run([
            "mvn", "validate", "-T 1.5C", "-Dformat.check=true",
            "-Dformat.skipTestSources=false", "-Dformat.skipSources=false"
        ],
            check=True)

    def _ensure_third_party(self):
        if not Path("third-party").exists():
            self.third_party()


class Make:
    """
    Command-Line Interface.
    """

    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Make health-dataset-generator',
            usage="make <command> [<args>]")
        subcommands = [
            attr for attr in dir(self)
            if not attr.startswith("_") and callable(getattr(self, attr))
        ]
        parser.add_argument(
            'command',
            help='Subcommand to run: one of ' + " ".join(subcommands))
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            exit(1)
        getattr(self, args.command)()

    def docker_run(self):
        parser = argparse.ArgumentParser(description='Run the docker image')
        parser.add_argument(
            '-b',
            '--build',
            dest='build',
            action='store_true',
            help='build the image beforehand')
        parser.add_argument(
            '-v', '--volume', action='append', help='A volume to mount')
        parser.add_argument(
            '--docker',
            action='append',
            help='Extra parameter to pass to "docker run". Example: ' +
            '--docker=net=app_default')
        args = parser.parse_args(sys.argv[2:])
        self._docker().docker_run(
            build=args.build,
            volumes=args.volume or [],
            extra_parameters=args.docker or [])

    def docker_build(self):
        parser = argparse.ArgumentParser(description='Build the docker image')
        args = parser.parse_args(sys.argv[2:])
        self._docker().docker_build()

    def docker_push(self):
        parser = argparse.ArgumentParser(description='Push the docker image')
        args = parser.parse_args(sys.argv[2:])
        self._docker().docker_push()

    def third_party(self):
        parser = argparse.ArgumentParser(
            description="Download/compile third-party dependencies and " +
            "put them in the 'third-party' directory")
        args = parser.parse_args(sys.argv[2:])
        self._dev().third_party()

    def test(self):
        parser = argparse.ArgumentParser(description="Execute tests")
        args = parser.parse_args(sys.argv[2:])
        self._dev().test()

    def format(self):
        parser = argparse.ArgumentParser(description='Format source files')
        args = parser.parse_args(sys.argv[2:])
        self._dev().format()

    def format_check(self):
        parser = argparse.ArgumentParser(
            description='Check source file formatting')
        args = parser.parse_args(sys.argv[2:])
        self._dev().format_check()

    def _docker(self):
        # We enable buildkit by default
        with_buildkit = True

        # We enable --cache-from by default
        with_cache_from = True
        if os.environ.get("CIRCLECI"):
            # Docker version is too old on CircleCI to support --cache-from.
            # See https://github.com/moby/buildkit/issues/569 for the
            # underlying issue.
            with_cache_from = False

        return Docker(
            with_buildkit=with_buildkit, with_cache_from=with_cache_from)

    def _dev(self):
        return Dev(self._docker())


if __name__ == "__main__":
    try:
        Make()
    except KeyboardInterrupt as e:
        print("Interrupted")
        sys.exit(1)
