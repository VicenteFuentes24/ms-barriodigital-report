FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
RUN addgroup -S report && adduser -S report -G report
COPY --chown=report:report target/ms-report-0.0.1-SNAPSHOT.jar /app/app.jar
USER report
ENV SERVER_ADDRESS=0.0.0.0
ENV JAVA_TOOL_OPTIONS="-XX:MaxRAMPercentage=60 -XX:+ExitOnOutOfMemoryError"
EXPOSE 8083
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
