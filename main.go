package main

import (
	"crypto/md5"
	"fmt"
	"os"
	"runtime"

	"log"

	"strconv"
	"time"

	"github.com/go-redis/redis"
	"github.com/nats-io/go-nats"
)

const (
	defaultCacheTime = 500
	defaultNatsURL   = "nats://localhost:4222"
	defaultRedisAddr = "localhost:6379"
	defaultRedisDB   = 0
)

var (
	rc        *redis.Client
	nc        *nats.Conn
	cacheTime int
)

func main() {
	var err error

	// Setup NATS
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = defaultNatsURL
	}

	nc, err = nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Unable to connect to nats server: %v", err)
	}

	defer nc.Close()

	// Setup Redis
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = defaultRedisAddr
	}

	rc = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: os.Getenv("REDIS_PASS"),
		DB:       defaultRedisDB,
	})

	// Setup Redis Cache Time
	ct := os.Getenv("CACHE_TIME")
	if ct == "" {
		cacheTime = defaultCacheTime
	} else {
		cacheTime, err = strconv.Atoi(ct)
		if err != nil {
			log.Fatalf("Error while converting CACHE_TIME to int: %v", err)
		}
	}

	// Market Orders
	nc.Subscribe("marketorders.raw", func(m *nats.Msg) {
		hash := md5.Sum(m.Data)
		key := fmt.Sprintf("%v-%v", m.Subject, hash)

		if !is_duped_message(key) {
			log.Print("Publishing deduped market order...")
			nc.Publish("marketorders.deduped", m.Data)
		} else {
			log.Print("Got a duplicated market order...")
		}
	})

	// Gold Prices
	nc.Subscribe("goldprices.raw", func(m *nats.Msg) {
		hash := md5.Sum(m.Data)
		key := fmt.Sprintf("%v-%v", m.Subject, hash)

		if !is_duped_message(key) {
			log.Print("Publishing deduped set of gold prices...")
			nc.Publish("goldprices.deduped", m.Data)
		} else {
			log.Print("Got a duplicated set of gold prices...")
		}
	})

	runtime.Goexit()
}

func is_duped_message(key string) bool {
	_, err := rc.Get(key).Result()
	if err == redis.Nil {
		set(key)

		// It didn't exist so not a duped message
		return false
	} else if err != nil {
		fmt.Printf("Error while getting from Redis: %v", err)

		// There was a problem with Redis and since we cannot verify
		// if the message is a dupe lets just say it isn't. Better
		// safe than sorry.
		return false
	} else {
		set(key)

		// There was no problem with Redis and we got a value back.
		// So it was a dupe.
		return true
	}
}

func set(key string) {
	cache_time := time.Duration(cacheTime) * time.Second

	_, err := rc.Set(key, 1, cache_time).Result()
	if err != nil {
		fmt.Printf("Something wrong seting redis key: %v", err)
	}
}
