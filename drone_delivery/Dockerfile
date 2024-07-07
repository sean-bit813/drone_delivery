# Stage 1: Build the application
FROM maven:3.8.4-openjdk-11 AS build

# Set the working directory inside the container
WORKDIR /app

# Copy the pom.xml file and download the dependencies
COPY pom.xml ./
RUN mvn dependency:go-offline

# Copy the entire project and build the application
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Create the runtime image
FROM openjdk:11-jre-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the built jar from the build stage
COPY --from=build /app/target/drone_delivery-1.0-SNAPSHOT.jar ./drone_delivery.jar

# Expose the application port
EXPOSE 8080

# Command to run the application
CMD ["java", "-jar", "drone_delivery.jar"]
