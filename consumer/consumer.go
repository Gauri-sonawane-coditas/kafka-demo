package main

import (
    "context"
    "encoding/json"
    "log"
    "net/http"

    "github.com/gin-gonic/gin"
    "github.com/segmentio/kafka-go"
)

type Comment struct {
    Text string `json:"text"`
}

func main() {
    // Start the Gin server
    app := gin.Default()

    // Endpoint to consume comments
    app.GET("/consumer", consumeCommentsHandler)

    // Run the server
    go app.Run(":8081")
	
    // Start the consumer
    go consumeComments("10.88.0.2:9092", "comments")

    // Prevent main from exiting
    select {}
}

func consumeCommentsHandler(ctx *gin.Context) {
    ctx.JSON(http.StatusOK, gin.H{"message": "Consumer is running and listening for messages"})
}

func consumeComments(brokerAddress, topic string) {
    // Create a new Kafka reader
    reader := kafka.NewReader(kafka.ReaderConfig{
        Brokers:  []string{brokerAddress},
        GroupID:  "consumer-group-id",
        Topic:    topic,
        MinBytes: 10e3, // 10KB
        MaxBytes: 10e6, // 10MB
    })
    defer reader.Close()

    for {
        // Read message
        message, err := reader.ReadMessage(context.Background())
        if err != nil {
            log.Fatalf("could not read message: %v", err)
        }

        // Process the message
        log.Printf("received: %s = %s\n", string(message.Key), string(message.Value))

        // Here you could unmarshal the message.Value into a Comment struct if needed
        var comment Comment
        err = json.Unmarshal(message.Value, &comment)
        if err != nil {
            log.Printf("could not unmarshal message: %v", err)
            continue
        }

        log.Printf("comment: %s\n", comment.Text)
    }
}