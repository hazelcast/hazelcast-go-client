package main

import (
	"log"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

func createDestroyMap(client *hazelcast.Client, name string) error {
	log.Println("Creating a map.")
	m, err := client.GetMap("t1")
	if err != nil {
		return err
	}
	time.Sleep(1 * time.Second)
	log.Println("Destroying the map.")
	if err = m.Destroy(); err != nil {
		return err
	}
	return nil
}

func main() {
	client, err := hazelcast.StartNewClient()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Adding the event listener.")
	subID, err := client.AddDistributedObjectListener(func(e hazelcast.DistributedObjectNotified) {
		log.Println(e.EventType, e.ServiceName, e.ObjectName)
	})
	if err != nil {
		log.Fatal(err)
	}
	// creating a map an destroying it afterwards
	if err = createDestroyMap(client, "my-map-1"); err != nil {
		log.Fatal(err)
	}
	log.Println("Removing the event listener.")
	if err := client.RemoveDistributedObjectListener(subID); err != nil {
		log.Fatal(err)
	}
	log.Println("The event listener was removed, no DistributedObject events will be received from now on...")
	// creating a map an destroying it afterwards
	if err = createDestroyMap(client, "my-map-2"); err != nil {
		log.Fatal(err)
	}
}
