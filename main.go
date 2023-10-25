package main

import (
	"errors"
	"fmt"
	"log"
	"os"

	db "github.com/help-me-someone/scalable-p2-db"
	"github.com/help-me-someone/scalable-p2-db/models/user"
	"github.com/help-me-someone/scalable-p2-db/models/video"
	"github.com/help-me-someone/scalable-p2-worker/worker"
	"github.com/hibiken/asynq"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Region for the s3 server.
const (
	REGION = "sgp1"
)

var (
	DB_USERNAME string
	DB_PASSWORD string
	DB_IP       string
	REDIS_IP    string
	MODE        string
)

func loadEnvs() {
	DB_USERNAME = os.Getenv("DB_USERNAME")
	DB_PASSWORD = os.Getenv("DB_PASSWORD")
	DB_IP = os.Getenv("DB_IP")
	REDIS_IP = os.Getenv("REDIS_IP")
	MODE = os.Getenv("MODE")
}

func main() {
	// Load from environment variables.
	loadEnvs()

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

	redisArr := fmt.Sprintf("%s:6379", REDIS_IP)
	clientOpt := asynq.RedisClientOpt{Addr: redisArr}

	dsn := fmt.Sprintf(
		"%s:%s@tcp(%s)/toktik-db?charset=utf8mb4&parseTime=True&loc=Local",
		DB_USERNAME,
		DB_PASSWORD,
		DB_IP,
	)
	toktik_db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Panicln("Error: Failed to connect to the database.")
	}

	// Initalize the database, incase it's not ready.
	if !toktik_db.Migrator().HasTable(&user.User{}) && !toktik_db.Migrator().HasTable(&video.Video{}) {
		db.InitTables(toktik_db)
		log.Println("Database initialized!")
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
