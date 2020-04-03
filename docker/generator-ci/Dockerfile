FROM circleci/openjdk:8-buster

## Install sbt
ENV SBT_VERSION 1.3.8
RUN wget https://dl.bintray.com/sbt/debian/sbt-$SBT_VERSION.deb && \
    dpkg -i sbt-$SBT_VERSION.deb && \
    rm sbt-$SBT_VERSION.deb && \
    apt-get update && \
    apt-get install -y sbt

RUN apt-get update && apt-get install -y python3