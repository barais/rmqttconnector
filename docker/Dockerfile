# ---- builder ----
FROM rust:1.90-slim-bookworm as builder

# Install required libs for building (openssl headers for sqlx/openssl)
RUN apt-get update && \
    apt-get install -y --no-install-recommends libssl-dev pkg-config ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app

# Copy Cargo manifest first to cache dependencies
COPY Cargo.toml Cargo.lock ./

# If you use workspace or extra files update accordingly
# Create a dummy src to allow `cargo fetch`/`cargo build` caching when deps change
RUN mkdir -p src && echo "fn main(){}" > src/main.rs

# Fetch and build dependencies (speeds up rebuilds)
RUN cargo fetch

# Now copy full source
COPY . .

# Build in release
RUN cargo build --release

# ---- runtime ----
FROM debian:bookworm-slim

# runtime deps: ca-certificates + libssl
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates libssl3 && \
    rm -rf /var/lib/apt/lists/*

# Create non-root user (optional but recommended)
RUN useradd --system --create-home appuser

# Copy binary and mapping file(s)
# Adjust the binary path/name if your package name differs
COPY --from=builder /usr/src/app/target/release/mqtt_to_timescale /usr/local/bin/mqtt_to_timescale
COPY --from=builder /usr/src/app/mappings.json /etc/mqtt_to_timescale/mappings.json

WORKDIR /etc/mqtt_to_timescale
RUN chown -R appuser:appuser /etc/mqtt_to_timescale

ENV RUST_LOG=info
# If you want to set a default mappings file location inside the container:
ENV MAPPINGS_FILE=/etc/mqtt_to_timescale/mappings.json

USER appuser

# Container runs the binary. Configuration is via env vars (see README).
CMD ["/usr/local/bin/mqtt_to_timescale"]
