FROM alpine:3.20.2

WORKDIR /app

COPY nats-jetstream-to-elastic /usr/bin/
ENTRYPOINT ["/usr/bin/nats-jetstream-to-elastic"]
