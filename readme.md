# NATS JetStream to Elastic

`nats-jetstream-to-elastic` is a service that consumes messages from a NATS JetStream stream and indexes them into an Elasticsearch cluster. The project is intended to streamline data ingestion pipelines that require NATS JetStream as a messaging system and Elasticsearch as a searchable log store.

## Features

- Consumes messages from NATS JetStream
- Indexes data to Elasticsearch with customizable templates
- Supports retry on failure and bulk indexing
- Flexible configuration using YAML
- Docker image available: [`paragor/nats-jetstream-to-elastic`](https://hub.docker.com/r/paragor/nats-jetstream-to-elastic)

## Prerequisites

- **NATS JetStream** for message streaming
- **Elasticsearch v8.x** for indexing messages

## Getting Started

### Docker

The simplest way to run this service is via Docker:

```bash
docker run -d \
  -e ELASTICSEARCH_USERNAME="your-username" \
  -e ELASTICSEARCH_PASSWORD="your-password" \
  -v /path/to/config.yaml:/app/config.yaml \
  paragor/nats-jetstream-to-elastic
```

### Configuration

The service is configured via a YAML file. Example configuration [./config.yaml](./config.yaml)

### Running Locally

To run the service locally, clone the repository and build the project:

```bash
git clone https://github.com/paragor/nats-jetstream-to-elastic
cd nats-jetstream-to-elastic
go build -o nats-jetstream-to-elastic
./nats-jetstream-to-elastic -config=config.yaml
```

Make sure you have your `config.yaml` in place as per the above example.

### Environment Variables

- `ELASTICSEARCH_USERNAME`: (Optional) Username for Elasticsearch.
- `ELASTICSEARCH_PASSWORD`: (Optional) Password for Elasticsearch.
- `LOG_LEVEL`: Set the log level (debug, info, warn, error). Default is `info`.

### Signals

The service gracefully handles system signals (`SIGINT`, `SIGTERM`) and ensures that all in-flight messages are either acknowledged or rejected before shutting down.

## NATS JetStream Configuration

You must configure your NATS JetStream streams and consumers properly. The service expects the following:

- Stream and consumer should exist in the NATS JetStream system.
- The stream should contain messages that can be indexed into Elasticsearch.

## Elasticsearch Configuration

Ensure that your Elasticsearch cluster is accessible and supports bulk indexing. The service uses bulk requests to optimize indexing performance. Customize the `flushInterval` and `flushBytes` in the configuration to tune performance.

## Logging

The service uses `zap` logging. You can configure the log level using the `LOG_LEVEL` environment variable.
