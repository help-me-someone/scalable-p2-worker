package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/hibiken/asynq"
	ffmpeg "github.com/u2takey/ffmpeg-go"
)

type TaskHandler struct {
	Client *asynq.Client
}

func (h *TaskHandler) WithContext(task func(ctx context.Context, t *asynq.Task) error) func(ctx context.Context, t *asynq.Task) error {
	return func(ctx context.Context, t *asynq.Task) error {
		return task(context.WithValue(ctx, "client", h.Client), t)
	}
}

// A list of task types.
const (
	TypeVideoSave      = "video:save"
	TypeVideoThumbnail = "video:thumbnail"
)

type VideoSavePayload struct {
	UserID    string
	VideoName string
}

type VideoThumbnailPayload struct {
	UserID    string
	VideoName string
}

//----------------------------------------------
// Write a function NewXXXTask to create a task.
// A task consists of a type and a payload.
//----------------------------------------------

func NewVideoSaveTask(userID string, videoName string) (*asynq.Task, error) {
	payload, err := json.Marshal(VideoSavePayload{UserID: userID, VideoName: videoName})
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(TypeVideoSave, payload), nil
}

func NewVideoThumbnailTask(userID string, videoName string) (*asynq.Task, error) {
	payload, err := json.Marshal(VideoThumbnailPayload{UserID: userID, VideoName: videoName})
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(TypeVideoThumbnail, payload), nil
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface.
//---------------------------------------------------------------

func HandleVideoSaveTask(ctx context.Context, t *asynq.Task) error {
	var p VideoSavePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Job Description: We download the file, then we convert and save it back into the same place
	// we downloaded it from.

	// Step 1. Make a new client.
	// For aws.
	awsClient, err := makeAWSClient(region)
	if err != nil {
		return fmt.Errorf("failed to create aws client: %v: %w", err, asynq.SkipRetry)
	}
	// For asynq.
	qC := ctx.Value("client")
	queueClient, ok := qC.(*asynq.Client)
	if qC == nil || !ok {
		return fmt.Errorf("failed to retrieve asynq client: %v: %w", err, asynq.SkipRetry)
	}

	// Step 2. Create a downloader.
	downloader := manager.NewDownloader(awsClient)

	// Step 3. Declare a new file, which will hold our downloaded file.
	filePath := "temp/convert/" + p.VideoName
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer os.Remove(filePath)
	defer file.Close()

	// Step 4. Download the file.
	_, err = downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(p.VideoName),
		})
	if err != nil {
		return err
	}

	// Step 5. Apply ffmpeg conversion, we convert everything to mp4.
	convertedFilePath := filePath + ".mp4"
	err = ffmpeg.Input(filePath).Output(convertedFilePath).OverWriteOutput().ErrorToStdOut().Run()
	if err != nil {
		return err
	}

	// Setp 6. Create an uploader.
	uploader := manager.NewUploader(awsClient)

	// Step 7. Retrieve the output file.
	convertedFile, err := os.Open(convertedFilePath)
	if err != nil {
		return err
	}
	defer os.Remove(convertedFilePath)
	defer convertedFile.Close()

	// Step 8. Upload the file back.
	expires := time.Now().AddDate(0, 0, 1)
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:  aws.String("toktik-videos"),
		Key:     aws.String(p.VideoName), // We save it using the same address.
		Body:    convertedFile,
		Expires: &expires,
	})
	if err != nil {
		return err
	}

	// Step 9. Queue the next job.
	t1, err := NewVideoThumbnailTask(p.UserID, p.VideoName)
	if err != nil {
		return err
	}

	// Queue the task.
	_, err = queueClient.Enqueue(t1)
	if err != nil {
		return err
	}

	log.Println("Successfully uploaded converted file to bucket")

	return nil
}

func readFrameJpeg(inFileName string, frameNum int) (io.Reader, error) {
	buf := bytes.NewBuffer(nil)
	err := ffmpeg.Input(inFileName).
		Filter("thumbnail", ffmpeg.Args{}).
		Output("pipe:", ffmpeg.KwArgs{"vframes": 1, "format": "image2", "vcodec": "mjpeg"}).
		WithOutput(buf, os.Stdout).
		Run()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func HandleVideoThumbnailTask(ctx context.Context, t *asynq.Task) error {
	var p VideoThumbnailPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Job Description: We download the file, then we create a thumbnail and save it onto s3,
	// using the same name but with a fixed postfix we use for thumbnails. This works
	// because each of our key is going to have a fixed length anyways.

	// Step 1. Make a new client.
	// For aws.
	awsClient, err := makeAWSClient(region)
	if err != nil {
		return fmt.Errorf("failed to create aws client: %v: %w", err, asynq.SkipRetry)
	}

	// Step 2. Create a downloader.
	downloader := manager.NewDownloader(awsClient)

	// Step 3. Declare a new file, which will hold our downloaded file.
	filePath := "temp/thumbnail/" + p.VideoName + ".mp4"
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer os.Remove(filePath)
	defer file.Close()

	// Step 4. Download the file.
	_, err = downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(p.VideoName),
		})
	if err != nil {
		return err
	}

	// Step 5. Create a thumbnail and load into buffer.
	thumbnail, err := readFrameJpeg(filePath, 1)
	if err != nil {
		return err
	}

	// Setp 6. Create an uploader.
	uploader := manager.NewUploader(awsClient)

	// Step 7. Upload the image.
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String("toktik-videos"),
		Key:         aws.String(p.VideoName + "-thumbnail"), // We save it using the same address.
		Body:        thumbnail,
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return err
	}

	log.Println("Successfully created a thumbnail")

	return nil
}
