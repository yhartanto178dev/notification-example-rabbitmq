package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Constants
const (
	rabbitMQURL    = "amqp://guest:guest@127.0.0.1:5672/"
	mongoDBURL     = "mongodb://127.0.0.1:27017"
	databaseName   = "notification_service"
	collectionName = "notifications"
	queueName      = "notifications"
)

// Notification represents a notification structure
type Notification struct {
	ID      string    `json:"id,omitempty" bson:"_id,omitempty"`
	Message string    `json:"message" bson:"message"`
	Status  string    `json:"status" bson:"status"` // "unread" or "read"
	Created time.Time `json:"created" bson:"created"`
}

// Global variables
var rabbitConn *amqp.Connection
var rabbitChannel *amqp.Channel
var mongoClient *mongo.Client
var notificationCollection *mongo.Collection

func main() {
	// Initialize Echo
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Initialize RabbitMQ and MongoDB
	initRabbitMQ()
	defer rabbitConn.Close()
	defer rabbitChannel.Close()

	initMongoDB()
	defer mongoClient.Disconnect(context.Background())

	// Routes
	e.POST("/notifications", sendNotification)
	e.GET("/notifications", readNotifications)
	e.POST("/notifications/:id/status", updateNotificationStatus)

	// Start the server
	e.Logger.Fatal(e.Start(":8080"))
}

// initRabbitMQ initializes RabbitMQ
func initRabbitMQ() {
	var err error
	rabbitConn, err = amqp.Dial(rabbitMQURL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}

	rabbitChannel, err = rabbitConn.Channel()
	if err != nil {
		log.Fatalf("Failed to create RabbitMQ channel: %v", err)
	}

	_, err = rabbitChannel.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare RabbitMQ queue: %v", err)
	}
}

// initMongoDB initializes MongoDB
func initMongoDB() {
	var err error
	mongoClient, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoDBURL))
	if err != nil {
		log.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	notificationCollection = mongoClient.Database(databaseName).Collection(collectionName)
}

// sendNotification handles sending a notification
func sendNotification(c echo.Context) error {
	notification := new(Notification)
	if err := c.Bind(notification); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request payload"})
	}

	notification.Status = "unread"
	notification.Created = time.Now()

	// Save to MongoDB
	res, err := notificationCollection.InsertOne(context.Background(), notification)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to save notification"})
	}

	// Publish to RabbitMQ
	notificationJSON, _ := json.Marshal(notification)
	err = rabbitChannel.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        notificationJSON,
		},
	)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to publish notification"})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message":      "Notification sent successfully",
		"notification": notification,
		"id":           res.InsertedID,
	})
}

// readNotifications retrieves notifications from MongoDB
func readNotifications(c echo.Context) error {
	cursor, err := notificationCollection.Find(context.Background(), bson.M{})
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch notifications"})
	}
	defer cursor.Close(context.Background())

	notifications := []Notification{}
	for cursor.Next(context.Background()) {
		var notification Notification
		if err := cursor.Decode(&notification); err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to decode notification"})
		}
		notifications = append(notifications, notification)
	}

	return c.JSON(http.StatusOK, notifications)
}

// updateNotificationStatus updates the status of a notification
func updateNotificationStatus(c echo.Context) error {
	id := c.Param("id")
	newStatus := struct {
		Status string `json:"status" validate:"required"`
	}{}

	if err := c.Bind(&newStatus); err != nil || (newStatus.Status != "read" && newStatus.Status != "unread") {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid status"})
	}

	filter := bson.M{"_id": id}
	update := bson.M{"$set": bson.M{"status": newStatus.Status}}

	result, err := notificationCollection.UpdateOne(context.Background(), filter, update)
	if err != nil || result.MatchedCount == 0 {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to update notification"})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Notification status updated"})
}
