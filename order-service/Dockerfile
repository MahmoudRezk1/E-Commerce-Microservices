FROM maven:3.9.9-eclipse-temurin-17 AS build

WORKDIR /app

COPY . .

RUN mvn clean package -pl order-service -am -DskipTests

FROM eclipse-temurin:17-jdk

WORKDIR /app

COPY --from=build /app/order-service/target/*.jar order.jar

EXPOSE 8081

ENTRYPOINT ["java", "-jar", "order.jar"]