# Build Valkey module with Automerge 3.1.2 from source
FROM rust:1 AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y clang git && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Clone Automerge 3.1.2 from GitHub
RUN git clone --depth 1 --branch js/automerge-3.1.2 https://github.com/automerge/automerge.git /build/automerge-src

# Copy our valkey-automerge module code
COPY valkey-automerge/ ./valkey-automerge/

# Update Cargo.toml to use automerge from the cloned git repo
RUN cd valkey-automerge && \
  sed -i 's|automerge = ".*"|automerge = { path = "/build/automerge-src/rust/automerge" }|' Cargo.toml

# Build the Valkey module
RUN cargo build --release --manifest-path valkey-automerge/Cargo.toml

# Runtime image with Valkey and the compiled module
FROM valkey/valkey:8.0
COPY --from=builder /build/valkey-automerge/target/release/libvalkey_automerge.so /usr/lib/valkey/modules/valkey-automerge.so
CMD ["valkey-server", \
  "--loadmodule", "/usr/lib/valkey/modules/valkey-automerge.so", \
  "--loglevel", "notice", \
  "--logfile", "", \
  "--slowlog-log-slower-than", "0", \
  "--slowlog-max-len", "128", \
  "--notify-keyspace-events", "KEA", \
  "--dir", "/data", \
  "--save", "", \
  "--appendonly", "yes", \
  "--appendfilename", "appendonly.aof", \
  "--appendfsync", "everysec", \
  "--aof-use-rdb-preamble", "no", \
  "--enable-debug-command", "yes"]
