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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hibiken/asynq"
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

	clientOpt := asynq.RedisClientOpt{Addr: "localhost:6379"}
	srv := asynq.NewServer(
		clientOpt,
		asynq.Config{Concurrency: 10},
	)

	client := asynq.NewClient(clientOpt)
	taskHandler := &TaskHandler{Client: client}

	mux := asynq.NewServeMux()
	mux.HandleFunc(TypeVideoSave, taskHandler.WithContext(HandleVideoSaveTask))
	mux.HandleFunc(TypeVideoThumbnail, taskHandler.WithContext(HandleVideoThumbnailTask))
	mux.HandleFunc(TypeVideoConvertMPD, taskHandler.WithContext(HandleVideoConvertMPDTask))

	if err := srv.Run(mux); err != nil {
		log.Fatal(err)
	}
}
