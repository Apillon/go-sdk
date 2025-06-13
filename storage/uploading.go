// Package storage provides functions to handle file uploads to Apillon storage buckets.
// It manages the upload session lifecycle, including starting uploads, uploading files via signed URLs, and ending sessions.
package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/Apillon/go-sdk/requests"
)

const (
	defaultContentType = "text/plain"
	urlReadyDelay     = 2 * time.Second
)

// StartUploadFilesToBucket initiates an upload session for a set of files in a given bucket.
// It sends file metadata to the Apillon API and returns the raw API response or an error.
func StartUploadFilesToBucket(ctx context.Context, bucketUuid string, files []FileMetadata) (string, error) {
	if bucketUuid == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID cannot be empty",
		}
	}

	if len(files) == 0 {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "no files provided for upload",
		}
	}

	// Ensure each file has a content type
	for i := range files {
		if files[i].ContentType == "" {
			files[i].ContentType = defaultContentType
		}
		if files[i].FileName == "" {
			return "", &StorageError{
				Code:    ErrCodeInvalidInput,
				Message: fmt.Sprintf("file at index %d has no name", i),
			}
		}
	}

	reqBody := startUploadRequest{Files: files}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: "failed to marshal upload files request",
			Err:     err,
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/upload"
	res, err := requests.PostReq(ctx, path, strings.NewReader(string(bodyBytes)))
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: "failed to start upload session",
			Err:     err,
		}
	}

	return res, nil
}

// UploadFiles uploads a file's raw content to a signed URL using HTTP PUT.
// Returns a success message or an error if the upload fails.
func UploadFiles(ctx context.Context, signedURL string, rawFile string) error {
	if signedURL == "" {
		return &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "signed URL cannot be empty",
		}
	}

	if rawFile == "" {
		return &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "file content cannot be empty",
		}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, signedURL, strings.NewReader(rawFile))
	if err != nil {
		return &StorageError{
			Code:    500,
			Message: "failed to create upload request",
			Err:     err,
		}
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return &StorageError{
			Code:    500,
			Message: "failed to upload file",
			Err:     err,
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return &StorageError{
			Code:    resp.StatusCode,
			Message: fmt.Sprintf("upload failed: %s", string(bodyBytes)),
		}
	}

	return nil
}

// EndSession finalizes an upload session for a given bucket and session ID.
// Returns the API response or an error.
func EndSession(ctx context.Context, bucketUuid string, sessionId string) (string, error) {
	if bucketUuid == "" || sessionId == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID and session ID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/upload/" + sessionId + "/end"
	res, err := requests.PostReq(ctx, path, nil)
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: "failed to end upload session",
			Err:     err,
		}
	}

	return res, nil
}

// UploadFileProcess orchestrates the full upload process for multiple files:
// 1. Starts an upload session and retrieves signed URLs.
// 2. Uploads each file to its corresponding signed URL.
// 3. Ends the upload session.
// Returns the final API response or an error.
func UploadFileProcess(ctx context.Context, bucketUuid string, files []WholeFile) (string, error) {
	if bucketUuid == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID cannot be empty",
		}
	}

	if len(files) == 0 {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "no files provided for upload",
		}
	}

	// Extract only the metadata for the upload session initiation
	onlyMetadata := make([]FileMetadata, len(files))
	for i, file := range files {
		if file.Content == "" || file.Metadata.FileName == "" {
			return "", &StorageError{
				Code:    ErrCodeInvalidInput,
				Message: fmt.Sprintf("file content or metadata is empty for file %s", file.Metadata.FileName),
			}
		}
		onlyMetadata[i] = file.Metadata
	}

	// Step 1: Start upload session and get signed URLs
	res, err := StartUploadFilesToBucket(ctx, bucketUuid, onlyMetadata)
	if err != nil {
		return "", fmt.Errorf("failed to start upload session: %w", err)
	}

	var apiResp ProcessAPIResponse
	if err := json.Unmarshal([]byte(res), &apiResp); err != nil {
		return "", &StorageError{
			Code:    500,
			Message: "failed to unmarshal process upload response",
			Err:     err,
		}
	}

	// Extract signed URLs from API response
	var urls []string
	if apiResp.Data.Files != nil {
		for _, fileItem := range apiResp.Data.Files {
			if fileItem.URL != "" {
				urls = append(urls, fileItem.URL)
			}
		}
	}

	if len(urls) == 0 {
		return "", &StorageError{
			Code:    500,
			Message: "no signed URLs found in process upload response",
		}
	}

	if len(urls) < len(files) {
		return "", &StorageError{
			Code:    500,
			Message: fmt.Sprintf("not enough signed URLs provided. Expected %d, got %d", len(files), len(urls)),
		}
	}

	// Wait for the URLs to be ready
	time.Sleep(urlReadyDelay)

	// Step 2: Upload each file to its signed URL
	for i, file := range files {
		if err := UploadFiles(ctx, urls[i], file.Content); err != nil {
			return "", fmt.Errorf("failed to upload file %s: %w", file.Metadata.FileName, err)
		}
	}

	// Step 3: End the upload session
	res, err = EndSession(ctx, bucketUuid, apiResp.Data.SessionUUID)
	if err != nil {
		return "", fmt.Errorf("failed to end upload session: %w", err)
	}

	return res, nil
}
