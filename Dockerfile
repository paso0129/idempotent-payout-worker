FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle gradle
# Dependency layer: re-downloaded only when the build files change.
RUN ./gradlew --no-daemon --quiet dependencies > /dev/null
COPY src src
RUN ./gradlew --no-daemon --quiet bootJar

FROM eclipse-temurin:21-jre
RUN useradd --system --uid 10001 payout
WORKDIR /app
COPY --from=build /workspace/build/libs/app.jar app.jar
USER payout
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
