FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY . .

RUN mvn clean package -pl notification-service -am -DskipTests

FROM eclipse-temurin:17-jdk

WORKDIR /app

COPY --from=build /app/notification-service/target/*.jar notification.jar

EXPOSE 8082

ENTRYPOINT ["java", "-jar", "notification.jar"]