use anyhow::Result;
use dotenv::dotenv;
use rand::Rng;
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, QoS};
use serde::Deserialize;
use serde_json::Value;
use sqlx::Pool;
use sqlx::Postgres;
use std::env;
use std::fs;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Deserialize, Clone)]
struct TopicMapping {
    topic: String, // peut contenir + et #
    user_id: i64,  // device_id à insérer
}

/// Charge le fichier de mappings JSON
fn load_mappings(path: &str) -> Result<Vec<TopicMapping>> {
    let s = fs::read_to_string(path)?;
    let v: Vec<TopicMapping> = serde_json::from_str(&s)?;
    Ok(v)
}

/// Vérifie si un topic reçu correspond à un filtre MQTT contenant + et #
/// filter peut être "sensors/+/events" ou "plants/#"
fn topic_matches(filter: &str, topic: &str) -> bool {
    // Split into levels
    let f_parts: Vec<&str> = filter.split('/').collect();
    let t_parts: Vec<&str> = topic.split('/').collect();

    let mut i = 0usize;
    loop {
        if i >= f_parts.len() && i >= t_parts.len() {
            return true;
        }
        if i >= f_parts.len() {
            // filter ended but topic longer => no match
            return false;
        }

        let fp = f_parts[i];

        if fp == "#" {
            // multi-level wildcard: matches everything remaining
            return true;
        } else if fp == "+" {
            // single-level wildcard: must have a topic level here
            if i >= t_parts.len() {
                return false;
            }
            // continue
        } else {
            // exact match required
            if i >= t_parts.len() || fp != t_parts[i] {
                return false;
            }
        }
        i += 1;
    }
}

/// Crée un pool sqlx PgPool (orm)
async fn create_pg_pool(database_url: &str) -> Result<Pool<Postgres>, sqlx::Error> {
    // sqlx::postgres::PgPoolOptions::new().max_connections(5).connect(database_url).await
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
}

/// Reconnect postgres (backoff exponentiel)
async fn reconnect_pg(database_url: &str) -> Pool<Postgres> {
    let mut delay = 1u64;
    loop {
        match create_pg_pool(database_url).await {
            Ok(pool) => {
                println!("✅ Reconnected to Postgres (sqlx)");
                return pool;
            }
            Err(e) => {
                eprintln!(
                    "⚠️ Failed to connect to Postgres (sqlx): {}. Retry in {}s",
                    e, delay
                );
                sleep(Duration::from_secs(delay)).await;
                delay = std::cmp::min(delay * 2, 60);
            }
        }
    }
}
/// Connecte MQTT (création client + eventloop)
fn create_mqtt(
    host: &str,
    port: u16,
    user: Option<&str>,
    pass: Option<&str>,
    client_id: &str,
) -> (AsyncClient, EventLoop) {
    let mut mqttoptions = MqttOptions::new(client_id, host, port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    if let (Some(u), Some(p)) = (user, pass) {
        mqttoptions.set_credentials(u, p);
    }
    AsyncClient::new(mqttoptions, 10)
}

/// Reconnection MQTT avec backoff et re-subscribe à la liste de topics
async fn reconnect_mqtt(
    host: &str,
    port: u16,
    user: Option<&str>,
    pass: Option<&str>,
    client_id: &str,
    topics: &[String],
) -> (AsyncClient, EventLoop) {
    let mut delay = 1;
    loop {
        let (client, eventloop) = create_mqtt(host, port, user, pass, client_id);
        // essaie de subscribe à tous les topics pour valider la connexion
        let mut ok = true;
        for t in topics {
            if let Err(e) = client.subscribe(t, QoS::AtMostOnce).await {
                eprintln!("⚠️ MQTT subscribe to '{}' failed: {}", t, e);
                ok = false;
                break;
            }
        }
        if ok {
            println!(
                "✅ Reconnected to MQTT and subscribed to {} topics",
                topics.len()
            );
            return (client, eventloop);
        } else {
            eprintln!("⚠️ Will retry MQTT connect in {}s...", delay);
            sleep(Duration::from_secs(delay)).await;
            delay = std::cmp::min(delay * 2, 60);
        }
    }
}

/// Boucle principale : reçoit messages, choisit device_id selon mapping, insère en DB.
/// Si DB ou MQTT tombent, tentatives de reconnexion avec backoff.
async fn run(
    mqtt_host: String,
    mqtt_port: u16,
    mqtt_user: Option<String>,
    mqtt_pass: Option<String>,
    mqtt_client_id: String,
    mappings: Vec<TopicMapping>,
    pg_conn: String,
) -> Result<()> {
    // Prépare la liste de topics à subscribe (unique)
    let mut topics: Vec<String> = Vec::new();
    for m in &mappings {
        if !topics.contains(&m.topic) {
            topics.push(m.topic.clone());
        }
    }

    // Connect Postgres et MQTT
    // create pg pool
    let mut pool = create_pg_pool(&pg_conn)
        .await
        .map_err(|e| anyhow::anyhow!("initial pg pool error: {}", e))?;
    //    let mut pg_client = connect_postgres(&pg_conn).await?;
    let (mut mqtt_client, mut eventloop) = create_mqtt(
        &mqtt_host,
        mqtt_port,
        mqtt_user.as_deref(),
        mqtt_pass.as_deref(),
        &mqtt_client_id,
    );
    // Subscribe à tous les topics
    for t in &topics {
        mqtt_client.subscribe(t, QoS::AtMostOnce).await?;
    }
    println!("✅ Subscribed to {} MQTT topics", topics.len());

    loop {
        match eventloop.poll().await {
            Ok(Event::Incoming(Packet::Publish(publish))) => {
                // Convert payload -> &str
                if let Ok(text) = std::str::from_utf8(&publish.payload) {
                    // find mapping that matches this publish.topic (first match)
                    let mut mapped_device: Option<i64> = None;
                    for m in &mappings {
                        if topic_matches(&m.topic, &publish.topic) {
                            mapped_device = Some(m.user_id.clone());
                            break;
                        }
                    }

                    // Si mapping trouvé -> on utilise mapped_device. Sinon, on essaie de prendre device_id depuis JSON
                    let user_id_to_use = if let Some(dev) = mapped_device {
                        dev
                    } else {
                        rand::thread_rng().gen_range(0..1000)
                    };

                    // parse JSON and try to extract device_id field, fallback to publish.topic as device_id
                    match serde_json::from_str::<Value>(text) {
                        Ok(json_val) => {
                            let device_id_to_use = json_val
                                .get("device_id")
                                .and_then(|v| v.as_str())
                                .map(|s| s.to_owned())
                                .unwrap_or_else(|| user_id_to_use.to_string());

                            let q = "INSERT INTO metrics(time, user_id, device_id, data) VALUES (NOW(), $1, $2, $3::jsonb)";
                            if let Err(e) = sqlx::query(&q)
                                .bind(&user_id_to_use)
                                .bind(&device_id_to_use)
                                .bind(json_val)
                                .execute(&pool)
                                .await
                            {
                                eprintln!("❌ DB insert error: {}", e);
                                // On suppose que l'erreur peut être due à une déconnexion : reconnect
                                pool = reconnect_pg(&pg_conn).await;
                            } else {
                                println!(
                                    "✅ Inserted (device_id='{}') from topic='{}'",
                                    device_id_to_use, publish.topic
                                );
                            }
                        }
                        Err(_) => {
                            // Si JSON invalide, utiliser user_id_to_use comme device_id
                            println!(
                                "⚠️ Invalid JSON payload on topic '{}', using device_id='{}'",
                                publish.topic, user_id_to_use
                            );
                        }
                    };

                //                    let v: Value = serde_json::from_str(text)?;
                // Re-sérialiser pour garantir format canonical (optionnel)
                //                   let _ = serde_json::to_string(&v)?;
                // Insert into DB using device_id_to_use
                } else {
                    eprintln!("⚠️ Payload not utf-8 on topic '{}'", publish.topic);
                }
            }

            Err(e) => {
                eprintln!("⚠️ MQTT eventloop error: {}. Reconnecting...", e);
                let (new_client, new_loop) = reconnect_mqtt(
                    &mqtt_host,
                    mqtt_port,
                    mqtt_user.as_deref(),
                    mqtt_pass.as_deref(),
                    &mqtt_client_id,
                    &topics,
                )
                .await;
                mqtt_client = new_client;
                eventloop = new_loop;
            }

            _ => {
                // On ignore autres événements (Outgoing, ConnAck, etc.)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "localhost".into());
    let mqtt_port: u16 = env::var("MQTT_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1883);
    let mqtt_user = env::var("MQTT_USER").ok();
    let mqtt_pass = env::var("MQTT_PASS").ok();
    let mqtt_client_id = env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "mqtt_to_timescale".into());
    //  let pg_conn =
    //      env::var("PG_CONN").unwrap_or_else(|_| "host=localhost port=5434 user=postgres password=password dbname=postgres".into());
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost:5434/postgres".to_string());

    // Fichier de mappings (par défaut "mappings.json")
    let mappings_file = env::var("MAPPINGS_FILE").unwrap_or_else(|_| "mappings.json".into());
    println!("🔎 Loading topic mappings from '{}'", mappings_file);
    let mappings = match load_mappings(&mappings_file) {
        Ok(m) => m,
        Err(e) => {
            eprintln!("❌ Failed to load mappings file '{}': {}", mappings_file, e);
            std::process::exit(1);
        }
    };
    println!("✅ Loaded {} mappings", mappings.len());

    println!("🚀 Starting MQTT→Timescale bridge...");
    run(
        mqtt_host,
        mqtt_port,
        mqtt_user,
        mqtt_pass,
        mqtt_client_id,
        mappings,
        database_url,
    )
    .await
}
