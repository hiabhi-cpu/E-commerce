package main

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"
)

// Define connection constants for KRaft mode
const (
	kafkaBroker = "localhost:9092"
	topic       = "orders"
)

func main() {
	// Initialize Kafka Writer
	writer := &kafka.Writer{
		Addr:         kafka.TCP(kafkaBroker),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll, // Ensure message is committed
	}
	defer writer.Close()

	r := gin.Default()

	// POST endpoint
	r.POST("/orders", func(c *gin.Context) {
		handleOrderRequest(c, writer)
	})

	r.Run(":8001") // Order service runs on 8001
}

// handleOrderRequest processes the JSON and produces to Kafka
func handleOrderRequest(c *gin.Context, writer *kafka.Writer) {
	var order Order

	// 1. Bind JSON to struct
	if err := c.ShouldBindJSON(&order); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid order format"})
		return
	}

	// 2. Convert the entire struct to a JSON byte slice
	orderBytes, err := json.Marshal(order)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to serialize order"})
		return
	}

	// 3. Prepare Kafka message
	msg := kafka.Message{
		Key:   []byte(order.OrderID),
		Value: orderBytes, // You could also marshal the whole struct to JSON
		Time:  time.Now(),
	}

	// 4. Write to Kafka
	err = writer.WriteMessages(context.Background(), msg)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to place order in queue"})
		return
	}

	// 5. Success Response
	c.JSON(http.StatusAccepted, gin.H{
		"status":   "Order Placed",
		"order_id": order.OrderID,
	})
}
