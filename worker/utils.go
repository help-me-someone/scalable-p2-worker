package worker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hibiken/asynq"
)

//----------------------------------------------
// A basic function to help create an AWS client.
//----------------------------------------------

func MakeAWSClient(region string) (*s3.Client, error) {
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

//----------------------------------------------
// Middleware for logging tasks.
//----------------------------------------------

func LoggingMiddleware(h asynq.Handler) asynq.Handler {
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
