-- Create example table for CDC demo.
-- Debezium will emit changes from this table into Kafka.

CREATE TABLE IF NOT EXISTS customers (
  id INT NOT NULL,
  name VARCHAR(255) NOT NULL,
  email VARCHAR(255) NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);

INSERT INTO customers (id, name, email) VALUES
  (1, 'Alice', 'alice@example.com'),
  (2, 'Bob', 'bob@example.com')
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  email = VALUES(email);

-- Grant the Debezium user the privileges required for binlog reading.
-- The container MYSQL_USER/MYSQL_PASSWORD are already created by the image.
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT SELECT ON streamforge.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

