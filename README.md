# MQTT → TimescaleDB Bridge (Rust + SQLX)

A lightweight Rust service that:
- Connects to an **MQTT broker** using authentication,
- Subscribes to multiple **topics** defined in a JSON mapping file,
- Receives structured JSON messages from devices,
- Inserts them into a **TimescaleDB** (PostgreSQL) database as JSONB data,
- Automatically **reconnects** to both MQTT and PostgreSQL in case of connection loss.

---

## 🧩 Features

- ✅ MQTT authentication (username/password)
- ✅ Dynamic topic–device mapping loaded from JSON
- ✅ Automatic reconnection to MQTT and PostgreSQL
- ✅ Asynchronous I/O powered by **Tokio**
- ✅ SQL operations with **SQLX**
- ✅ Compatible with **TimescaleDB hypertables**

---

## 🗂️ Table Schema

You should have a table similar to:

```sql
CREATE TABLE metrics (
  time TIMESTAMPTZ,
  user_id INT,
  device_id VARCHAR(256),
  data JSONB
);

SELECT create_hypertable('metrics', by_range('time'));
```


## ⚙️ Configuration

The application loads its configuration from environment variables and a mapping file.

| Variable         | Description                                 | Default                                               |
| ---------------- | ------------------------------------------- | ----------------------------------------------------- |
| `MQTT_HOST`      | MQTT broker hostname                        | `localhost`                                           |
| `MQTT_PORT`      | MQTT broker port                            | `1883`                                                |
| `MQTT_USER`      | MQTT username                               | *(optional)*                                          |
| `MQTT_PASS`      | MQTT password                               | *(optional)*                                          |
`mqtt_to_timescale_sqlx`                              |
| `DATABASE_URL`   | PostgreSQL / TimescaleDB connection string  | `postgres://postgres:postgres@localhost:5432/metrics` |
| `APP_USER_ID`    | The user ID inserted in the `metrics` table | `1`                                                   |
| `MAPPINGS_FILE`  | Path to topic-to-device mapping JSON file   | `mappings.json`                                       |


## 🧭 Topic Mapping File (mappings.json)

Defines which MQTT topics correspond to which device IDs.

Example:

```json
[
  { "topic": "devices/heater/1", "user_id": 1 },
  { "topic": "devices/heater/2", "user_id": 2 },
  { "topic": "sensors/+/events", "user_id": 1 },
  { "topic": "plants/#", "user_id": 2 }
]
```

You can use MQTT wildcards (+, #) to match multiple topics.


## 🏗️ Build & Run

Prerequisites

- Rust (edition 2024, Rust ≥ 1.70)
- A running MQTT broker (e.g., Mosquitto, HiveMQ)
- A TimescaleDB / PostgreSQL instance


#### Clone and build

```bash
git clone https://github.com/yourusername/mqtt-to-timescale-sqlx.git
cd mqtt-to-timescale-sqlx
cargo build --release
```

### Run

```bash
cp .env.example .env  # or set environment variables manually
cargo run --release
```

## 🧱 Example Insert

When a message like this is received on a subscribed topic:

```json
{
  "device_id": "c4282185-499e-4743-84b3-697b05068ffc",
  "energy": 4403342,
  "temp1": 12.8,
  "temp2": 33.9,
  "temp3": 36.2,
  "pump1": 0,
  "pump2": 0,
  "wifi": 100
}
```

It is stored in TimescaleDB as:

```sql
INSERT INTO metrics (time, user_id, device_id, data)
VALUES (NOW(), 1, 'c4282185-499e-4743-84b3-697b05068ffc', '{"energy":4403342,"temp1":12.8,...}'::jsonb);
```

## 🔄 Reconnection Logic

If the MQTT connection drops, the client automatically reconnects and re-subscribes to all topics.

If the PostgreSQL connection fails, the pool is recreated with exponential backoff, and the failed message is retried once.

## 📦 Dependencies

- tokio
- rumqttc
- sqlx
- serde
- serde_json
- dotenv
- anyhow

## 🧰 Example .env


```txt
MQTT_HOST=broker.emqx.io
MQTT_PORT=1883
MQTT_USER=myuser
MQTT_PASS=mypassword
DATABASE_URL=postgres://postgres:postgres@localhost:5432/metrics
MAPPINGS_FILE=mappings.json
```

## 🧩 Future Improvements

- [ ] Buffered storage for messages during DB outages
- [ ] Structured logging with tracing
- [x] Dockerfile and docker-compose
- [x] systemd service unit
- [ ] Healthcheck endpoint for observability


## 🪪 License
MIT License © 2025
Created by [Olivier Barais]