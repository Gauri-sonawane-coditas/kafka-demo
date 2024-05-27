package main

import (
    "context"
    "fmt"
    "log"

    "github.com/gin-gonic/gin"
    "github.com/segmentio/kafka-go"
)

type Comment struct {
    Text string `json:"text"`
}

func main() {
    app := gin.Default()
    app.POST("/producer", createComment)

    // Start the Gin server
    app.Run(":8080")
}

func createComment(ctx *gin.Context) {
    var comment Comment
    if err := ctx.ShouldBindJSON(&comment); err != nil {
        ctx.JSON(400, gin.H{"error": "there was some error in parsing"})
        return
    }

    brokerAddress := "10.88.0.2:9092"

    topic := "comments"

    err := PushCommentToQueue(brokerAddress, topic, comment)
    if err != nil {
        ctx.JSON(500, gin.H{"error": fmt.Sprintf("error pushing comment to queue: %v", err)})
        return
    }

    ctx.JSON(200, gin.H{"message": "comment pushed to queue successfully"})
}

func PushCommentToQueue(brokerAddress, topic string, comment Comment) error {
    // Create a new context
    ctx := context.Background()

    // Create a new Kafka writer
    writer := kafka.NewWriter(kafka.WriterConfig{
        Brokers:  []string{brokerAddress},
        Topic:    topic,
        Balancer: &kafka.LeastBytes{},
    })
    defer writer.Close()

    // Construct the Kafka message
    message := kafka.Message{
        Key:   []byte("Key"),
        Value: []byte(comment.Text),
    }

    // Send the message
    err := writer.WriteMessages(ctx, message)
    if err != nil {
        return fmt.Errorf("could not write message: %w", err)
    }

    log.Printf("sent: %s\n", comment.Text)
    return nil
}