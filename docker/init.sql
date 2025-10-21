CREATE TABLE metrics (
  time TIMESTAMPTZ,
  user_id INT,
  device_id VARCHAR(256),
  data JSONB
);

SELECT create_hypertable('metrics', by_range('time'));