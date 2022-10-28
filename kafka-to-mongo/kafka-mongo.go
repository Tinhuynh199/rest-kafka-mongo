package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"gopkg.in/mgo.v2"
)

const (
	hosts      = "localhost:27017"
	database   = "testdb"
	username   = ""
	password   = ""
	collection = "jobs"
)

type Job struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	Company     string `json:"company"`
	Salary      string `json:"salary"`
}

type MongoStore struct {
	session *mgo.Session
}

var mongoStore = MongoStore{}

func main() {

	// Create MongoDB session
	session := initializeMongo()
	mongoStore.session = session

	receiveFromKafka()
}

func initializeMongo() (session *mgo.Session) {

	info := &mgo.DialInfo{
		Addrs:    []string{hosts},
		Timeout:  60 * time.Second,
		Database: database,
		Username: username,
		Password: password,
	}

	session, err := mgo.DialWithInfo(info)
	if err != nil {
		panic(err)
	}

	return
}

func receiveFromKafka() {

	fmt.Println("Start receiving from Kafka")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "group-id-1",
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		panic(err)
	}

	consumer.SubscribeTopics([]string{"jobs-topic1"}, nil)

	for {
		msg, err := consumer.ReadMessage(-1)

		if err == nil {
			fmt.Printf("Received from Kafka %s: %s\n", msg.TopicPartition, string(msg.Value))
			job := string(msg.Value)
			saveJobToMongo(job)
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
			break
		}
	}

	consumer.Close()

}

func saveJobToMongo(jobString string) {

	// Save job data to mongo
	fmt.Println("Save data to MongoDB")
	col := mongoStore.session.DB(database).C(collection)

	// Save data into Job struct
	var _job Job
	body := []byte(jobString)
	err := json.Unmarshal(body, &_job)
	if err != nil {
		panic(err)
	}

	// Insert job data into MongoDB
	errMongo := col.Insert(_job)
	if errMongo != nil {
		panic(errMongo)
	}

	fmt.Printf("Save data to MongoDB: %s\n", jobString)
}
