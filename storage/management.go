package storage

import (
	"context"
	"encoding/json"
	"strings"

	"github.com/Apillon/go-sdk/requests"
)

// CreateBucketRequest represents the request body for creating a bucket
type CreateBucketRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// CreateBucket creates a new storage bucket with the specified name and optional description.
// Sends a POST request to the storage API to create the bucket.
// Returns an error if the request fails or the API returns an error.
func CreateBucket(ctx context.Context, name string, description string) error {
	if name == "" {
		return &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket name cannot be empty",
		}
	}

	reqBody := CreateBucketRequest{
		Name:        name,
		Description: description,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return &StorageError{
			Code:    500,
			Message: "failed to marshal create bucket request",
			Err:     err,
		}
	}

	_, err = requests.PostReq(ctx, "/storage/buckets", strings.NewReader(string(bodyBytes)))
	if err != nil {
		return &StorageError{
			Code:    500,
			Message: "failed to create bucket",
			Err:     err,
		}
	}

	return nil
}

// GetBucket retrieves information about storage buckets, optionally filtered by name.
// Sends a GET request to the storage API with the provided name as a query parameter.
// Returns a ListBucketsResponse containing the bucket(s) information, or an error if the request or unmarshalling fails.
func GetBucket(ctx context.Context, name string) (ListBucketsResponse, error) {
	params := map[string]string{}
	if name != "" {
		params["name"] = name
	}

	res, err := requests.GetReq(ctx, "/storage/buckets/", params)
	if err != nil {
		return ListBucketsResponse{}, &StorageError{
			Code:    500,
			Message: "failed to get bucket",
			Err:     err,
		}
	}

	var bucketList ListBucketsResponse
	if err := json.Unmarshal([]byte(res), &bucketList); err != nil {
		return ListBucketsResponse{}, &StorageError{
			Code:    500,
			Message: "failed to unmarshal bucket list response",
			Err:     err,
		}
	}

	return bucketList, nil
}
