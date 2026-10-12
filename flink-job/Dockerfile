FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src src
# tests need Docker (Testcontainers); they run in CI / `mvn verify`, not inside the image build
RUN mvn -B -q package -DskipTests

FROM flink:2.2.1-java17
COPY --from=build /src/target/wiki-pipeline.jar /opt/flink/usrlib/wiki-pipeline.jar
