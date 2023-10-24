// TODO: I need to rename to task to video convert instead of save.
//
//
//
//
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hibiken/asynq"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// Region for the s3 server.
const (
	region = "sgp1"
)

//----------------------------------------------
// A basic function to help create an AWS client.
//----------------------------------------------

func makeAWSClient(region string) (*s3.Client, error) {
	// Make resolver for digital space.
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		return aws.Endpoint{
			URL: "https://" + region + ".digitaloceanspaces.com",
		}, nil
	})

	// Apply configuration.
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithEndpointResolverWithOptions(customResolver),
	)
	if err != nil {
		return nil, fmt.Errorf("Error: Can't load config\n")
	}

	// Make sure that credentials are set.
	_, err = cfg.Credentials.Retrieve(context.TODO())
	if err != nil {
		return nil, fmt.Errorf("Error: No credentials set\n")
	}

	// Create and return the client.
	return s3.NewFromConfig(cfg), nil
}

func loggingMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		start := time.Now()
		log.Printf("Start processing %q", t.Type())
		err := h.ProcessTask(ctx, t)
		if err != nil {
			return err
		}
		log.Printf("Finished processing %q: Elapsed Time = %v", t.Type(), time.Since(start))
		return nil
	})
}

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
	awsClient, _ := makeAWSClient(region)

	taskHandler := &TaskHandler{
		Client:    client,
		Database:  toktik_db,
		AWSClient: awsClient,
	}

	mux := asynq.NewServeMux()
	mux.Use(loggingMiddleware)
	mux.Use(taskHandler.ContextMiddleware)
	mux.HandleFunc(TypeVideoSave, HandleVideoSaveTask)
	mux.HandleFunc(TypeVideoThumbnail, HandleVideoThumbnailTask)
	mux.HandleFunc(TypeVideoConvertHLS, HandleVideoConvertHLSTask)
	mux.HandleFunc(TypeVideoUpdateProgress, HandleVideoUpdateProgressTask)

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
