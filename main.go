package main

import (
	"crypto/md5"
	"flag"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/go-redis/redis"
	"github.com/nats-io/go-nats"
	"github.com/regner/albiondata-client/lib"
	"encoding/json"
)

var (
	rc        *redis.Client
	nc        *nats.Conn
	enc       *nats.EncodedConn
	cacheTime int
	natsURL   string
	redisAddr string
	redisPass string
)

func init() {
	flag.StringVar(
		&natsURL,
		"n",
		"nats://ingest.albion-data.com:4222",
		"NATS server to connect to.",
	)

	flag.StringVar(
		&redisAddr,
		"r",
		"localhost:6379",
		"Redis server to connect to.",
	)

	flag.StringVar(
		&redisPass,
		"p",
		"",
		"Redis password to use.",
	)

	flag.IntVar(
		&cacheTime,
		"c",
		500,
		"Time in seconds to cache entries for.",
	)

}

func main() {
	flag.Parse()

	var err error

	// Setup NATS
	nc, err = nats.Connect(natsURL)
	if err != nil {
		log.Fatalf("Unable to connect to nats server: %v", err)
	}

	enc, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatalf("Unable to establish encoded nats connection: %v", err)
	}

	defer nc.Close()

	// Setup Redis
	rc = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
		DB:       0,
	})

	// Market Orders
	marketCh := make(chan *nats.Msg, 64)
	marketSub, err := nc.ChanSubscribe("marketorders.ingest", marketCh)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer marketSub.Unsubscribe()

	// Gold Prices
	goldCh := make(chan *nats.Msg, 64)
	goldSub, err := nc.ChanSubscribe("goldprices.ingest", goldCh)
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

	runtime.Goexit()
}

func handleMarketOrder(msg *nats.Msg) {
	morders := &lib.MarketUpload{}
	if err := json.Unmarshal(msg.Data, morders); err != nil {
		fmt.Printf("%v\n", err)
	}

	for _, order := range morders.Orders {
		jb, _ := json.Marshal(order)
		hash := md5.Sum(jb)
		key := fmt.Sprintf("%v-%v", msg.Subject, hash)

		if !isDupedMessage(key) {
			log.Print("Publishing deduped market order...")
			enc.Publish("marketorders.deduped", jb)
		} else {
			log.Print("Got a duplicated set of gold prices...")
		}
	}
}

func handleGold(msg *nats.Msg) {
	hash := md5.Sum(msg.Data)
	key := fmt.Sprintf("%v-%v", msg.Subject, hash)

	if !isDupedMessage(key) {
		log.Print("Publishing deduped set of gold prices...")
		nc.Publish("goldprices.deduped", msg.Data)
	} else {
		log.Print("Got a duplicated set of gold prices...")
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
	cache_time := time.Duration(cacheTime) * time.Second

	_, err := rc.Set(key, 1, cache_time).Result()
	if err != nil {
		fmt.Printf("Something wrong seting redis key: %v", err)
	}
}
