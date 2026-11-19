FROM eclipse-temurin:21-jdk AS build
WORKDIR /build
COPY .mvn/wrapper/maven-wrapper.properties .mvn/wrapper/
COPY mvnw pom.xml ./
COPY src/main src/main
RUN sh ./mvnw -B -ntp -DskipTests package

FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN addgroup -S report && adduser -S report -G report && command -v wget >/dev/null
COPY --from=build --chown=report:report /build/target/ms-report-0.0.1-SNAPSHOT.jar /app/app.jar
USER report
ENV SERVER_ADDRESS=0.0.0.0
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
