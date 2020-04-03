FROM openjdk:8-buster as synthea

ENV GIT_COMMIT=abc9fc4a2eedf2ab221f68a96efcc80955718c21

RUN wget https://github.com/synthetichealth/synthea/archive/${GIT_COMMIT}.zip -q && \
  unzip -q ${GIT_COMMIT}.zip && \
  mv synthea-${GIT_COMMIT} /opt/synthea && \
  rm ${GIT_COMMIT}.zip

WORKDIR /opt/synthea

RUN ./gradlew uberJar

# ===

FROM ubuntu

COPY --from=synthea \
  /opt/synthea/build/libs/synthea-with-dependencies.jar \
  /opt/generator/third-party/synthea-with-dependencies.jar