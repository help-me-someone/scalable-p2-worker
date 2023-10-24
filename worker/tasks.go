package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/help-me-someone/scalable-p2-db/functions/crud"
	"github.com/hibiken/asynq"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	"gorm.io/gorm"
)

type TaskHandler struct {
	Client    *asynq.Client
	Database  *gorm.DB
	AWSClient *s3.Client
}

type Task func(ctx context.Context, t *asynq.Task) error

func (h *TaskHandler) WithContext(task Task) Task {
	return func(ctx context.Context, t *asynq.Task) error {
		ctx = context.WithValue(ctx, "client", h.Client)
		ctx = context.WithValue(ctx, "database", h.Database)
		ctx = context.WithValue(ctx, "aws", h.AWSClient)
		return task(ctx, t)
	}
}
func (th *TaskHandler) ContextMiddleware(h asynq.Handler) asynq.Handler {
	return asynq.HandlerFunc(func(ctx context.Context, t *asynq.Task) error {
		ctx = context.WithValue(ctx, "client", th.Client)
		ctx = context.WithValue(ctx, "database", th.Database)
		ctx = context.WithValue(ctx, "aws", th.AWSClient)
		return h.ProcessTask(ctx, t)
	})
}

// A list of task types.
const (
	TypeVideoSave           = "video:save"
	TypeVideoThumbnail      = "video:thumbnail"
	TypeVideoConvertHLS     = "video:chunk"
	TypeVideoUpdateProgress = "video:update"
)

type VideoSavePayload struct {
	UserID    string
	VideoName string
}

type VideoThumbnailPayload struct {
	UserID    string
	VideoName string
}

type VideoConvertHLSPayload struct {
	UserID    string
	VideoName string
}

type VideoUpdateProgressPayload struct {
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

func NewVideoConvertHLSTask(userID string, videoName string) (*asynq.Task, error) {
	payload, err := json.Marshal(VideoConvertHLSPayload{UserID: userID, VideoName: videoName})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeVideoConvertHLS, payload), nil
}

func NewVideoUpdateProgressTask(userID string, videoName string) (*asynq.Task, error) {
	payload, err := json.Marshal(VideoUpdateProgressPayload{
		UserID:    userID,
		VideoName: videoName,
	})
	if err != nil {
		return nil, err
	}
	return asynq.NewTask(TypeVideoUpdateProgress, payload), nil
}

//---------------------------------------------------------------
// Write a function HandleXXXTask to handle the input task.
// Note that it satisfies the asynq.HandlerFunc interface.
//
// Handler doesn't need to be a function. You can define a type
// that satisfies asynq.Handler interface.
//---------------------------------------------------------------

func HandleVideoSaveTask(ctx context.Context, t *asynq.Task) error {
	//
	// SET UP.
	//
	var p VideoSavePayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	log.Println("Saving file to bucket...")

	// Step 0. Create job folder. Removed automatically after the job is done.
	jobDir, err := os.MkdirTemp("temp/save/", "job")
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Job dir:", jobDir)
	defer os.RemoveAll(jobDir)

	// Job Description:
	// We download the file, then we convert it to mp4 and save
	// it back into the same place we downloaded it from.
	//___________________________________________________________

	// Step 1. Make a new client.
	// For aws.
	awsClient, ok := ctx.Value("aws").(*s3.Client)
	if !ok {
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
	filePath := fmt.Sprintf("%s/%s.%s", jobDir, p.VideoName, "mp4")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Step 4. Download the file.
	bucketDir := fmt.Sprintf("users/%s/videos/%s", p.UserID, p.VideoName)
	bucketVidPath := fmt.Sprintf("%s/vid", bucketDir)
	_, err = downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(bucketVidPath), // We save it using the same address.
		})
	if err != nil {
		return err
	}

	// Step 5. Apply ffmpeg conversion, we convert everything to mp4.
	convertedFilePath := filePath + ".mp4"
	err = ffmpeg.Input(filePath).Output(convertedFilePath).OverWriteOutput().Run()

	if err != nil {
		log.Printf("Failed to convert video to mp4")
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
		Key:     aws.String(bucketVidPath), // We save it using the same address.
		Body:    convertedFile,
		Expires: &expires,
	})
	if err != nil {
		return err
	}

	log.Println("Successfully uploaded converted file to bucket")

	//
	// Queue the next job. We generate the thumbnail
	// and do the conversion at the same time.
	//
	t1, err := NewVideoThumbnailTask(p.UserID, p.VideoName)
	_, err = queueClient.Enqueue(t1)
	if err != nil {
		log.Panicln("Failed the queue next task")
		return err
	}
	t2, err := NewVideoConvertHLSTask(p.UserID, p.VideoName)
	_, err = queueClient.Enqueue(t2)
	if err != nil {
		log.Panicln("Failed the queue next task")
		return err
	}

	// Queue the status update
	t3, err := NewVideoUpdateProgressTask(p.UserID, p.VideoName)
	_, err = queueClient.Enqueue(t3)

	return nil
}

func uploadDirToS3(dir string, username string, videoname string, uploader *manager.Uploader) error {
	return filepath.WalkDir(dir, func(path string, f os.DirEntry, err error) error {
		if f.Type().IsRegular() {

			relativePath := strings.TrimPrefix(path, dir)
			log.Println("Relative Path:", relativePath)

			file, err := os.Open(path)
			if err != nil {
				return err
			}
			defer file.Close()

			bucketFilePath := fmt.Sprintf("users/%s/videos/%s%s", username, videoname, relativePath)
			log.Println("Bucket Path", bucketFilePath)
			_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
				Bucket: aws.String("toktik-videos"),
				Key:    aws.String(bucketFilePath), // We save it using the same address.
				Body:   file,
			})

			if err != nil {
				return err
			}
		}
		return nil
	})
}

func HandleVideoConvertHLSTask(ctx context.Context, t *asynq.Task) error {
	log.Println("Handing convert")

	var p VideoConvertHLSPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Step 0. Create job folder. Removed automatically after the job is done.
	jobDir, err := os.MkdirTemp("temp/convert/", "job")
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Job dir:", jobDir)
	defer os.RemoveAll(jobDir)

	// Step 1. Make a new client.
	// For aws.
	awsClient, ok := ctx.Value("aws").(*s3.Client)
	if !ok {
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
	filePathMP4 := fmt.Sprintf("%s/%s.%s", jobDir, p.VideoName, "mp4")
	file, err := os.Create(filePathMP4)
	if err != nil {
		return err
	}
	defer file.Close()

	// Step 4. Download the file.
	bucketDir := fmt.Sprintf("users/%s/videos/%s", p.UserID, p.VideoName)
	bucketVidPath := fmt.Sprintf("%s/vid", bucketDir)
	_, err = downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(bucketVidPath),
		})
	if err != nil {
		return err
	}

	log.Println("Running ")

	// Decare the directory the HLS files will go into
	mpdDirPath, err := os.MkdirTemp(jobDir, "hls")
	if err != nil {
		log.Println("Failed to create mpd directory")
		return nil
	}
	// Step 5. HLS Path
	filePathHLSPath := fmt.Sprintf("%s/%s.%s", mpdDirPath, "vid", "m3u8")
	log.Println("HSL Path:", filePathHLSPath)

	err = ffmpeg.
		Input(filePathMP4).
		Output(filePathHLSPath, ffmpeg.KwArgs{
			"codec":         "copy",
			"start_number":  0,
			"hls_time":      5,
			"hls_list_size": 0,
			"f":             "hls",
		}).
		OverWriteOutput().
		Run()
	if err != nil {
		log.Printf("Error: Failed to convert mp4 to HLS")
		return err
	}

	// Setp 6. Create an uploader.
	uploader := manager.NewUploader(awsClient)

	// Step 7. Upload all files.
	err = uploadDirToS3(mpdDirPath, p.UserID, p.VideoName, uploader)
	if err != nil {
		log.Panicln("Failed to upload directory to s3:", err)
		return err
	}

	log.Println("Successfully uploaded converted files (HLS) to bucket")

	// Queue the status update
	t2, err := NewVideoUpdateProgressTask(p.UserID, p.VideoName)
	_, err = queueClient.Enqueue(t2)

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
	//
	// SET UP.
	//
	var p VideoThumbnailPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}
	log.Println("Creating thumbnail...")

	// Step 0. Create job folder. Removed automatically after the job is done.
	jobDir, err := os.MkdirTemp("temp/thumbnail/", "job")
	if err != nil {
		log.Println(err)
		return err
	}
	log.Println("Job dir:", jobDir)
	defer os.RemoveAll(jobDir)

	// Job Description:
	// We download the file, then we create a thumbnail and save
	// it onto s3, using the same name but with a fixed postfix
	// we use for thumbnails. This works because each of our key
	// is going to have a fixed length anyways.
	//___________________________________________________________

	// Step 1. Make a new client.
	// For aws.
	awsClient, ok := ctx.Value("aws").(*s3.Client)
	if !ok {
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

	// Step 3. Declare a new file, which will hold our ownloaded file.
	filePath := fmt.Sprintf("%s/%s.%s", jobDir, p.VideoName, "mp4")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Step 4. Download the file.
	bucketDir := fmt.Sprintf("users/%s/videos/%s", p.UserID, p.VideoName)
	bucketVidPath := fmt.Sprintf("%s/vid", bucketDir)
	_, err = downloader.Download(context.TODO(), file,
		&s3.GetObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(bucketVidPath),
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
	bucketThumbnailPath := fmt.Sprintf("%s/%s", bucketDir, "thumbnail")
	_, err = uploader.Upload(context.TODO(), &s3.PutObjectInput{
		Bucket:      aws.String("toktik-videos"),
		Key:         aws.String(bucketThumbnailPath),
		Body:        thumbnail,
		ContentType: aws.String("image/jpeg"),
	})
	if err != nil {
		return err
	}

	log.Println("Successfully created a thumbnail")

	// Queue the status update
	t1, err := NewVideoUpdateProgressTask(p.UserID, p.VideoName)
	_, err = queueClient.Enqueue(t1)
	if err != nil {
		return err
	}

	return nil
}

func HandleVideoUpdateProgressTask(ctx context.Context, t *asynq.Task) error {
	var p VideoUpdateProgressPayload
	if err := json.Unmarshal(t.Payload(), &p); err != nil {
		return fmt.Errorf("json.Unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// 1. Retrieve the database connection.
	connection, ok := ctx.Value("database").(*gorm.DB)
	if !ok {
		log.Println("Error: No database connection for task context.")
		return fmt.Errorf("No database connection in task's context.")
	}

	// 2. Update the status of the job.
	//    This is horrible since I'm just incrementing the number,
	//    but by doing it this way, the ordering of async jobs pretty
	//    much becomes irrelevant to me.
	err := crud.UpdateVideoStatusIncrementByKey(connection, p.VideoName)
	if err != nil {
		log.Println("Error: Failed to update video status.")
		return err
	}

	// Check the new status.
	v, _ := crud.GetVideoByKey(connection, p.VideoName)
	status := v.Status

	// Three means that it has generated the thumbnail and it has been chunked.
	// We can now safely clean up and delete the mp4 file.
	if status == 3 {
		//
		// Clean up. Remove the original file, since it won't be used anymore.
		//
		awsClient := ctx.Value("aws").(*s3.Client)
		bucketDir := fmt.Sprintf("users/%s/videos/%s", p.UserID, p.VideoName)
		bucketVidPath := fmt.Sprintf("%s/vid", bucketDir)
		_, err = awsClient.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: aws.String("toktik-videos"),
			Key:    aws.String(bucketVidPath),
		})
		if err != nil {
			log.Println("Failed to delete original video file", err)
			return err
		}
	}

	return nil
}
