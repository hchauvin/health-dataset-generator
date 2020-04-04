# syntax=docker/dockerfile:experimental@sha256:787107d7f7953cb2d95ee81cc7332d79aca9328129318e08fc7ffbd252a20656

# ==================================================================================================
# Build synthea

FROM openjdk:8-buster as synthea

ENV GIT_COMMIT=abc9fc4a2eedf2ab221f68a96efcc80955718c21

RUN wget https://github.com/synthetichealth/synthea/archive/${GIT_COMMIT}.zip -q && \
  unzip -q ${GIT_COMMIT}.zip && \
  mv synthea-${GIT_COMMIT} /opt/synthea && \
  rm ${GIT_COMMIT}.zip

WORKDIR /opt/synthea

RUN ./gradlew uberJar

# ==================================================================================================
# Gather third-party dependencies

FROM ubuntu as third-party

COPY --from=synthea \
  /opt/synthea/build/libs/synthea-with-dependencies.jar \
  /opt/generator/third-party/synthea-with-dependencies.jar

# ==================================================================================================
# Build the base image, used for compiling and testing the project itself

FROM polynote/polynote:0.3.6-2.12@sha256:5c3f5aaf4851a548d72ecd5554c71d788973cf13f74decbb096288daebaceb1f as project-base

USER root

RUN wget http://apache.crihan.fr/dist/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz -q && \
  tar zxpf spark-3.0.0-preview2-bin-hadoop2.7.tgz && mv spark-3.0.0-preview2-bin-hadoop2.7 /opt/spark && rm spark-3.0.0-preview2-bin-hadoop2.7.tgz

ENV SPARK_HOME=/opt/spark

RUN wget https://mirrors.ircam.fr/pub/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz -q && \
  tar zxpf apache-maven-3.6.3-bin.tar.gz && mv apache-maven-3.6.3 /opt/mvn && rm apache-maven-3.6.3-bin.tar.gz

ENV PATH=/opt/mvn/bin:${PATH}

WORKDIR /opt/generator

COPY --from=third_party /opt/generator/third-party /opt/generator/third-party

COPY . /opt/generator

# TODO: Build a test image

# ==================================================================================================
# Build the notebook image

FROM project-base

RUN --mount=type=cache,id=m2,target=/root/.m2 \
  --mount=type=cache,target=/opt/generator/generator-core/target \
  --mount=type=cache,target=/opt/generator/generator-source-synthea/target \
  --mount=type=cache,target=/opt/generator/generator-target-eds/target \
  mvn package -T 1.5C -Dmaven.test.skip=true -DskipTests && \
  mv /opt/generator/generator-target-eds/target/generator-target-eds-jar-with-dependencies.jar \
    /opt/generator/generator-target-eds-jar-with-dependencies.jar

WORKDIR /opt

ENV PATH=/opt/spark/bin:${PATH}

CMD [ "--config", "/opt/generator/notebooks/config.docker.yml" ]