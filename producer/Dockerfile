# Bridges Wikimedia's Server-Sent Events stream into Kafka: one JSON event per Kafka message.
FROM alpine:3.22
RUN apk add --no-cache curl kcat

ENV BOOTSTRAP=kafka:9092 \
    TOPIC=wiki-changes \
    STREAM_URL=https://stream.wikimedia.org/v2/stream/recentchange \
    USER_AGENT="realtime-streaming-demo/1.0 (https://github.com/DorukMaltepe/realtime_streaming)"

# SSE payload lines look like `data: {...}`; strip the prefix and pipe each JSON doc to kcat.
# The outer loop reconnects whenever Wikimedia closes the stream (it does, periodically).
CMD ["sh", "-c", "while true; do curl -sSN -A \"$USER_AGENT\" \"$STREAM_URL\" | awk '/^data: /{print substr($0, 7); fflush()}' | kcat -P -b \"$BOOTSTRAP\" -t \"$TOPIC\"; echo 'stream closed, reconnecting'; sleep 2; done"]
