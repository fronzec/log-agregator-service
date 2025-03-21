# Log Aggregator Service

A Spring Boot 3 application with Spring Batch for log aggregation.

## Requirements

- Java 21
- Maven 3.6+

## Building the Project

```bash
mvn clean install
```

## Running the Application

```bash
mvn spring-boot:run
```

The application will start on port 8080.

## Database Access

The application uses H2 in-memory database with MySQL compatibility mode. You can access the H2 web console at:

- URL: http://localhost:8080/h2-console
- JDBC URL: `jdbc:h2:mem:logdb`
- Username: `sa`
- Password: (leave empty)

Note: The database is configured in MySQL compatibility mode, which means you can use MySQL syntax in your queries.

## Technologies Used

- Spring Boot 3.2.3
- Spring Batch
- Java 21
- H2 Database (with MySQL compatibility)
- Maven
