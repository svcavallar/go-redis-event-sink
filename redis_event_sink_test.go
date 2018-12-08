package redis_event_sink

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	// Package dependencies
	"gopkg.in/redis.v5"
	log "github.com/Sirupsen/logrus"
)

var (
	redisClient    *redis.Client
	eventSink     *RedisEventSink
	redisURI    = "localhost:6379"
	redisDbIndex = 0
	redisPassword = ""
	testLifetime = 20 * time.Second
	redisOperationInterval = 250 * time.Millisecond
)

func TestNewRedisEventSink(t *testing.T) {
	var err error

	redisClient = redis.NewClient(&redis.Options{
		Addr:     redisURI,
		Password: redisPassword,
		DB:       redisDbIndex,
	})

	// Try and ping Redis to check connection status
	err = redisClient.Ping().Err()
	if err != nil {
		log.Errorf("Failed to connect to Redis database: %v", err)
	}

	log.Infof("Connected to Redis (URI: %s)", redisURI)

	eventSink = NewRedisEventSink(redisClient, 0, "*")

	//expect(t, err, nil)

	startTime := time.Now()

	keyWriteCount := 0
	eventSinkCount := 0

	// Go routine to monitor the events emitted on the redis event sink channel
	go func() {
		for {
			select {
			case ev := <-eventSink.EventChannel:
					log.Infof(`Event sunk => Key:"%s", Event:"%s"`, ev.Key, ev.KeyEvent)
					eventSinkCount++
			}
		}
	}()

	// Go routine to write temporary entries into redis at a regular time interval for the lifetime of the test
	go func() {
		dispatchStartTime := time.Now()

		for {
			dispatchElapsedTime := time.Since(dispatchStartTime)
			if dispatchElapsedTime.Seconds() >= redisOperationInterval.Seconds() {

				// Dummy data
				redisKeyValue := map[string]interface{}{
					"foo": "bar",
				}
				redisKeyValueJson, err := json.Marshal(redisKeyValue)
				if err != nil {
					log.Errorf("Error marshalling new redis key data")
				}

				redisKey := fmt.Sprintf("tmp:test_key:%d", keyWriteCount)

				log.Infof(`Writing expiring redis key: "%s"`, redisKey)
				// This will generate two events, one for SET and the other for EXPIRE
				_ = redisClient.Set(redisKey, redisKeyValueJson, testLifetime)
				keyWriteCount++

				dispatchStartTime = time.Now()
			}
		}
	}()

	// Get the events from the redis events sink channel
	for {
		elapsedTime := time.Since(startTime)
		if elapsedTime.Seconds() >= testLifetime.Seconds() {

			log.Printf("Generated %d keys", keyWriteCount)
			log.Printf("Sunk %d events", eventSinkCount)

			eventSink.Close()

			if 2*keyWriteCount != eventSinkCount {
				t.Errorf("Expected event sink count to be exactly double the count of keys generated")
			}

			break
		}
	}
}
