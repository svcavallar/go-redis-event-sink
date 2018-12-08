package redis_event_sink

import (
	"fmt"
	"strings"

	// Package dependencies
	log "github.com/Sirupsen/logrus"
	"gopkg.in/redis.v5"
)

// Constants
var constConfigKeyspaceEvents = "notify-keyspace-events"
var constKeyspaceFormat = "__keyevent@%d__"
var constKeyspacePatternFormat = "%s:%s"
var constDefaultKeyspacePattern = "*"

/*
RedisEventMsg is a message struct emitted onto the sink channel for a consumer
to process.
*/
type RedisEventMsg struct {
		// The redis key event, i.e. 'SET', 'EXPIRE' etc
		KeyEvent string
		// The key to which the event applies
		Key string
}

/*
RedisEventSink monitors events emitted from Redis data store.

NOTE: Events will not be emitted unless the Redis instance has the required
config entry set in the "redis.conf" file.

To do this, you can either:
	1. Ensure the following entry exists in your `redis.conf`:
  		notify-keyspace-events "AKE"
	2. Set the entry via `CONFIG SET`
	3. Use this event sink code to automatically check/set the config

For more detailed information about Redis keyspace notifications please
see http://redis.io/topics/notifications
*/
type RedisEventSink struct {
	redisClient     *redis.Client
	keyspace        string
	pattern         string
	keyspacePattern string
	subscriber      *redis.PubSub
	EventChannel   chan *RedisEventMsg
}

/*
NewRedisEventSink is a factory function that creates a new instance of the event sink
*/
func NewRedisEventSink(client *redis.Client, databaseIndex int64, pattern string) *RedisEventSink {

	if client == nil {
		log.Panic("[RedisEventSink] Invalid redis client passed")
	}

	if databaseIndex < 0 || databaseIndex > 16 {
		log.Panic("[RedisEventSink] Invalid database index specified")
	}

	if pattern == "" {
		pattern = constDefaultKeyspacePattern
	}

	var keyspace = fmt.Sprintf(constKeyspaceFormat, databaseIndex)

	self := &RedisEventSink{
		redisClient:     client,
		keyspace:        keyspace,
		pattern:         pattern,
		keyspacePattern: fmt.Sprintf(constKeyspacePatternFormat, keyspace, pattern),
		EventChannel:    make(chan *RedisEventMsg),
	}

	self.init()
	return self
}

func (sink *RedisEventSink) init() {
	var err error

  var setConfigKeyspaceEvents = false

	// Check to ensure keyspace events are enabled...
	configResultCmd := sink.redisClient.ConfigGet(constConfigKeyspaceEvents)
	if configResultCmd.Err() != nil {
		log.Panic(configResultCmd.Err())
	}

	if len(configResultCmd.Val()) > 0 {
		configValue := configResultCmd.Val()[1].(string)

		// Returned config string MUST contain at least a "K" or "E" in it
		if !strings.ContainsAny(configValue, "KE") {
			log.Warnf("[RedisEventSink] Keyspace event notifications (\"%s\") are not enabled correctly in Redis database config.", constConfigKeyspaceEvents)
      setConfigKeyspaceEvents = true
		}
	} else {
		log.Warnf("[RedisEventSink] Keyspace event notifications (\"%s\") are not enabled in Redis database config.", constConfigKeyspaceEvents)
    setConfigKeyspaceEvents = true
	}

	if setConfigKeyspaceEvents {
      log.Info("[RedisEventSink] Updating config for keyspace event notifications.")
      configStatusCmd := sink.redisClient.ConfigSet(constConfigKeyspaceEvents, "AKE")
      if configStatusCmd.Err() != nil {
          log.Panic(configStatusCmd.Err())
      }
  }

	// Subscribe to events matching the supplied keyspace pattern
	sink.subscriber, err = sink.redisClient.PSubscribe(sink.keyspacePattern)
	if err != nil {
		log.Panic(err)
	}

	log.Infof("[RedisEventSink] Subscribed to events - (Keyspace Pattern: \"%s\")", sink.keyspacePattern)

	// Start observing for events...
	go sink.listenForEvents()
}

/*
Pattern returns the pattern that this sink is observing
*/
func (sink *RedisEventSink) Pattern() string {
	return sink.pattern
}

/*
Close performs component clean up
*/
func (sink *RedisEventSink) Close() {
	if sink.subscriber != nil {
		err := sink.subscriber.Close()
		if err != nil {
			log.Errorf("[RedisEventSink] Error closing events subscription: %#v", err)
		}
	}
}

/*
listenForEvents receives events and pushes them back out on a channel
NOTE: Its easy to receive vast quantities of events, so consider commenting
out logging or making it conditional.
*/
func (sink *RedisEventSink) listenForEvents() {
	for {
		if sink.subscriber != nil {
			msgi, err := sink.subscriber.Receive()
			if err != nil {
				log.Errorf("[RedisEventSink] Error receiving events: %v", err)
			} else {
				// log.Infof("Received Redis Event: %#v", msgi)

				switch msg := msgi.(type) {
				case *redis.Subscription:
					log.Infof("[RedisEventSink] Event subscribed: \"%s\", Data:\"%s\"", msg.Kind, msg.Channel)
				case *redis.Message:
					eventType := strings.Replace(msg.Channel, sink.keyspace+":", "", -1)
					log.Debugf(`[RedisEventSink] Event:"%s", Channel:"%s", Payload:"%s"`, eventType, msg.Channel, msg.Payload)

					keyEventMsg := &RedisEventMsg{
						KeyEvent: eventType,
						Key: msg.Payload,
					}
					sink.EventChannel <- keyEventMsg

				default:
					log.Warnf("[RedisEventSink] Received unhandled event: %#v", msgi)
				}
			}
		}
	}
}
