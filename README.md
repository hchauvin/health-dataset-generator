# Synthetic health dataset generator

[![CircleCI](https://circleci.com/gh/hchauvin/health-dataset-generator/tree/master.svg?style=svg)](https://circleci.com/gh/hchauvin/health-dataset-generator/tree/master)

This project contains helpers to generate synthetic health datasets.

## Rationale

TODO

## Workflow

## Notebooks

There are only two dependencies necessary to launch the exploratory notebooks locally:

- Python 3.7+
- Docker 19.03+

The notebooks can be made available at `http://localhost:8192` with:

```bash
./make.py docker_run
```

It is not necesary to build anything to execute this command as the latest image is
fetched from Docker Hub.

To recompile the project before serving the notebooks, one can use the '-b/--build' flag:

```bash
./make.py docker_run -b
```

The following can be used to mount a volume inside the Docker container
(host volume "~/data", container volume "/opt/data"):

```bash
./make.py docker_run -b -v ~/data:/opt/data
```

## Development

- Test: `./make.py dev_test`
- Format source code: `./make.py dev_format`
- Check source code formatting: `./make.py dev_format_check`

