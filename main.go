package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/go-redis/redis"
	"github.com/nats-io/go-nats"
	"github.com/regner/albiondata-client/lib"
)

var (
	rc        *redis.Client
	nc        *nats.Conn
	cacheTime int
	natsURL   string
	redisAddr string
	redisPass string
)

func init() {
	flag.StringVar(
		&natsURL,
		"n",
		"nats://localhost:4222",
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

	defer nc.Close()

	// Setup Redis
	rc = redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPass,
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
