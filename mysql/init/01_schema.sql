USE aqi_telemetry;

CREATE TABLE IF NOT EXISTS telemetry (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50) NOT NULL,
    metric VARCHAR(50) NOT NULL,
    value DOUBLE NOT NULL,
    INDEX idx_timestamp (timestamp),
    INDEX idx_source_metric (source, metric)
);
