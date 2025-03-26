CREATE TABLE IF NOT EXISTS log_data (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp TIMESTAMP,
    level VARCHAR(10),
    service VARCHAR(50),
    message VARCHAR(500)
);
