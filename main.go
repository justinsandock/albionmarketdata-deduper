package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/kelseyhightower/envconfig"
	"github.com/nats-io/go-nats"
	"github.com/regner/albiondata-client/lib"
)

type config struct {
	CacheTime int    `default:"500"`
	NatsURL   string `default:"nats://localhost:4222"`
	RedisAddr string `default:"localhost:6379"`
	RedisPass string `default:""`
}

var (
	c  config
	rc *redis.Client
	nc *nats.Conn
)

func main() {
	err := envconfig.Process("deduper", &c)
	if err != nil {
		log.Fatal(err.Error())
	}

	// Setup NATS
	nc, err = nats.Connect(c.NatsURL)
	if err != nil {
		log.Fatalf("Unable to connect to nats server: %v", err)
	}

	defer nc.Close()

	// Setup Redis
	rc = redis.NewClient(&redis.Options{
		Addr:     c.RedisAddr,
		Password: c.RedisPass,
		DB:       0,
	})

	// Market Orders
	marketCh := make(chan *nats.Msg, 64)
	marketSub, err := nc.ChanSubscribe(lib.NatsMarketOrdersIngest, marketCh)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer marketSub.Unsubscribe()

	// Gold Prices
	goldCh := make(chan *nats.Msg, 64)
	goldSub, err := nc.ChanSubscribe(lib.NatsGoldPricesIngest, goldCh)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer goldSub.Unsubscribe()

	for {
		select {
		case msg := <-marketCh:
			handleMarketOrder(msg)
		case msg := <-goldCh:
			handleGold(msg)
		}
	}
}

func handleGold(msg *nats.Msg) {
	log.Print("Processing gold prices message...")

	hash := md5.Sum(msg.Data)
	key := fmt.Sprintf("%v-%v", msg.Subject, hash)

	if !isDupedMessage(key) {
		nc.Publish("goldprices.deduped", msg.Data)
	} else {
	}
}

func handleMarketOrder(msg *nats.Msg) {
	log.Print("Processing marker order message...")

	morders := &lib.MarketUpload{}
	if err := json.Unmarshal(msg.Data, morders); err != nil {
		fmt.Printf("%v\n", err)
	}

	for _, order := range morders.Orders {
		jb, _ := json.Marshal(order)
		hash := md5.Sum(jb)
		key := fmt.Sprintf("%v-%v", msg.Subject, hash)

		if !isDupedMessage(key) {
			nc.Publish("marketorders.deduped", jb)
		} else {
		}
	}
}

func isDupedMessage(key string) bool {
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
	cache_time := time.Duration(c.CacheTime) * time.Second

	_, err := rc.Set(key, 1, cache_time).Result()
	if err != nil {
		fmt.Printf("Something wrong seting redis key: %v", err)
	}
}
