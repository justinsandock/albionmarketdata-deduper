package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log"

	nats "github.com/nats-io/go-nats"
	"github.com/regner/albiondata-client/lib"
)

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
			nc.Publish("marketorders.deduped", jb)
		} else {
			log.Print("Got a duplicated set of gold prices...")
		}
	}
}
