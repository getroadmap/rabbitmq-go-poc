package main

import (
	"fmt"
	"log"
	"github.com/streadway/amqp"
	"encoding/csv"
	"strconv"
	"os"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when usused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)
	
	// create csv file
	file, err := os.Create("output_received.csv")
	if err != nil {
	log.Fatal(err)
	}
	
	// new csv writer
	writer := csv.NewWriter(file)
	
	// headers
    var new_headers = []string { "message_no", "message" }
    returnError := writer.Write(new_headers)
	writer.Flush()
    if returnError != nil {
        fmt.Println(returnError)
    }
	
	count := 1

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			
			num := strconv.Itoa(count)
			str := string(d.Body)
			
			var messages = []string { num, str } 
			returnError := writer.Write(messages)
			writer.Flush()
			if returnError != nil {
				fmt.Println(returnError)
			}		
			count++
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
