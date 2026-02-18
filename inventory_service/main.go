package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// 1. Configure the Reader
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "orders",
		GroupID:  "inventory-service-group", // This allows for scaling later
		MinBytes: 10e3,                      // 10KB
		MaxBytes: 10e6,                      // 10MB
	})
	defer reader.Close()

	fmt.Println("📦 Inventory Service started... waiting for orders.")

	for {
		// 2. Read the message from Kafka
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Printf("Error reading message: %v", err)
			continue
		}

		// 3. Unmarshal the JSON byte slice into our Order struct
		var order Order
		err = json.Unmarshal(m.Value, &order)
		if err != nil {
			log.Printf("Error decoding JSON: %v", err)
			continue
		}

		// 4. Print the order details to the console
		fmt.Printf("----------------------------------\n")
		fmt.Printf("NEW ORDER RECEIVED\n")
		fmt.Printf("Order ID:    %s\n", order.OrderID)
		fmt.Printf("Customer:    %s\n", order.CustomerID)
		fmt.Printf("Item:        %s\n", order.Item)
		fmt.Printf("Quantity:    %d\n", order.Quantity)
		fmt.Printf("Total Price: $%.2f\n", order.Amount)
		fmt.Printf("----------------------------------\n")
	}
}
