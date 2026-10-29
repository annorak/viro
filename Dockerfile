FROM eclipse-temurin:21-jre
WORKDIR /app
COPY target/viro.jar /app/viro.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/viro.jar"]
