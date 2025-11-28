FROM maven:3.9.0-eclipse-temurin-19  AS build

WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline
COPY src src
RUN mvn package

FROM eclipse-temurin:19

COPY --from=build /app/target/*.jar /high-load-course.jar

CMD ["java", "-jar", "/high-load-course.jar"]

