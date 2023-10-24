package main

import (
	"errors"
	"log"
	"os"

	"github.com/help-me-someone/scalable-p2-worker/worker"
	"github.com/hibiken/asynq"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Region for the s3 server.
const (
	REGION = "sgp1"
)

func main() {

	// Create temp folder for files to live in.
	paths := []string{"temp", "temp/convert", "temp/thumbnail", "temp/save"}
	for _, path := range paths {
		if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
			err := os.Mkdir(path, os.ModePerm)
			if err != nil {
				log.Println(err)
			}
		}
	}

	clientOpt := asynq.RedisClientOpt{Addr: "redis:6379"}

	// TODO: Load this via environments.
	dsn := "user:password@tcp(mysql:3306)/toktik-db?charset=utf8mb4&parseTime=True&loc=Local"
	toktik_db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Panicln("Error: Failed to connect to the database.")
	}

	srv := asynq.NewServer(
		clientOpt,
		asynq.Config{
			Concurrency: 10,
			IsFailure: func(err error) bool {
				log.Println(err)
				return err != nil
			},
		},
	)

	client := asynq.NewClient(clientOpt)
	awsClient, _ := worker.MakeAWSClient(REGION)

	taskHandler := &worker.TaskHandler{
		Client:    client,
		Database:  toktik_db,
		AWSClient: awsClient,
	}

	mux := asynq.NewServeMux()
	mux.Use(worker.LoggingMiddleware)
	mux.Use(taskHandler.ContextMiddleware)
	mux.HandleFunc(worker.TypeVideoSave, worker.HandleVideoSaveTask)
	mux.HandleFunc(worker.TypeVideoThumbnail, worker.HandleVideoThumbnailTask)
	mux.HandleFunc(worker.TypeVideoConvertHLS, worker.HandleVideoConvertHLSTask)
	mux.HandleFunc(worker.TypeVideoUpdateProgress, worker.HandleVideoUpdateProgressTask)

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
