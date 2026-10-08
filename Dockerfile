FROM eclipse-temurin:21-jdk AS build
WORKDIR /app
ADD https://repo.maven.apache.org/maven2/org/scala-sbt/sbt-launch/1.11.5/sbt-launch-1.11.5.jar /opt/sbt-launch.jar
COPY . .
RUN java -Dsbt.override.build.repos=true -Dsbt.repository.config=project/repositories -jar /opt/sbt-launch.jar assembly
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/scala-2.13/cdc-recovery-lab.jar /app/worker.jar
COPY fixtures /app/fixtures
RUN mkdir /app/.data && chown 10001:10001 /app/.data
USER 10001:10001
EXPOSE 8081
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=70", "-jar", "/app/worker.jar"]
CMD ["kafka"]
