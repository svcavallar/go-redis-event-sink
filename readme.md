# go-redis-event-sink

Golang helper package for subscribing and processing Redis keyspace events emitted on a channel.

## Dependencies

This library depends upon the following packages:

- github.com/Sirupsen/logrus
- gopkg.in/redis.v5

## Usage

Installation: `go get github.com/svcavallar/go-redis-event-sink`

This imports a new namespace called `redis_event_sink`

See `redis_event_sink_test.go` for full example of how to sink events from the channel.
