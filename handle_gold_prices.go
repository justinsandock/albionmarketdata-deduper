package main

import (
	"crypto/md5"
	"fmt"
	"log"

	"github.com/nats-io/go-nats"
)

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
