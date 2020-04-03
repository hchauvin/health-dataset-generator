# syntax=docker/dockerfile:experimental@sha256:787107d7f7953cb2d95ee81cc7332d79aca9328129318e08fc7ffbd252a20656
FROM polynote/polynote:0.3.6-2.12@sha256:5c3f5aaf4851a548d72ecd5554c71d788973cf13f74decbb096288daebaceb1f

USER root

RUN wget http://apache.crihan.fr/dist/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz -q && \
  tar zxpf spark-3.0.0-preview2-bin-hadoop2.7.tgz && mv spark-3.0.0-preview2-bin-hadoop2.7 /opt/spark && rm spark-3.0.0-preview2-bin-hadoop2.7.tgz

ENV SPARK_HOME=/opt/spark

RUN wget https://mirrors.ircam.fr/pub/apache/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz -q && \
  tar zxpf apache-maven-3.6.3-bin.tar.gz && mv apache-maven-3.6.3 /opt/mvn && rm apache-maven-3.6.3-bin.tar.gz

ENV PATH=/opt/mvn/bin:${PATH}

WORKDIR /opt/generator

COPY --from=eds-generator-third-party /opt/generator/third-party /opt/generator/third-party

COPY . /opt/generator

RUN --mount=type=cache,target=/root/.m2 \
  --mount=type=cache,target=/opt/generator/generator-core/target \
  --mount=type=cache,target=/opt/generator/generator-source-synthea/target \
  --mount=type=cache,target=/opt/generator/generator-target-eds/target \
  mvn package -Dmaven.test.skip=true -DskipTests

WORKDIR /opt

ENV PATH=/opt/spark/bin:${PATH}

CMD [ "--config", "/opt/generator/notebooks/config.docker.yml" ]