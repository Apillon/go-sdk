package storage

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Apillon/go-sdk/requests"
)

// StorageError represents an error that occurred during storage operations
type StorageError struct {
	Code    int
	Message string
	Err     error
}

func (e *StorageError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("storage error (code %d): %s: %v", e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("storage error (code %d): %s", e.Code, e.Message)
}

// Error codes
const (
	ErrCodeDirectoryNotFound = 40406003
	ErrCodeDirectoryDeleting = 40006007
	ErrCodeInvalidInput      = 40000001
)

// GetBucketContent retrieves the raw content of a storage bucket by its UUID.
// Returns the raw response as a string, or an error if the request fails.
func GetBucketContent(ctx context.Context, bucketUuid string) (string, error) {
	if bucketUuid == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/content"
	res, err := requests.GetReq(ctx, path, nil)
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to get bucket content for bucket %s", bucketUuid),
			Err:     err,
		}
	}

	return res, nil
}

// ListFilesInBucket lists all files in a given bucket by its UUID.
// Returns a ListFilesResponse struct or an error if the request or unmarshalling fails.
func ListFilesInBucket(ctx context.Context, bucketUuid string) (ListFilesResponse, error) {
	if bucketUuid == "" {
		return ListFilesResponse{}, &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/files"
	res, err := requests.GetReq(ctx, path, nil)
	if err != nil {
		return ListFilesResponse{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to list files in bucket %s", bucketUuid),
			Err:     err,
		}
	}

	var fileList ListFilesResponse
	if err := json.Unmarshal([]byte(res), &fileList); err != nil {
		return ListFilesResponse{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to unmarshal list files response for bucket %s", bucketUuid),
			Err:     err,
		}
	}

	return fileList, nil
}

// GetFileDetails retrieves details for a specific file in a bucket using their UUIDs.
// Returns a FileDetails struct or an error if the request or unmarshalling fails.
func GetFileDetails(ctx context.Context, bucketUuid string, fileUuid string) (FileDetails, error) {
	if bucketUuid == "" || fileUuid == "" {
		return FileDetails{}, &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID and file UUID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/files/" + fileUuid
	res, err := requests.GetReq(ctx, path, nil)
	if err != nil {
		return FileDetails{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to get file details for file %s in bucket %s", fileUuid, bucketUuid),
			Err:     err,
		}
	}

	var fileDetails FileDetails
	if err := json.Unmarshal([]byte(res), &fileDetails); err != nil {
		return FileDetails{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to unmarshal get file details response for file %s in bucket %s", fileUuid, bucketUuid),
			Err:     err,
		}
	}

	return fileDetails, nil
}

// DeleteFile deletes a specific file from a bucket using their UUIDs.
// Returns the raw response as a string, or an error if the request fails.
func DeleteFile(ctx context.Context, bucketUuid string, fileUuid string) (string, error) {
	if bucketUuid == "" || fileUuid == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID and file UUID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/files/" + fileUuid
	res, err := requests.DeleteReq(ctx, path)
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to delete file %s in bucket %s", fileUuid, bucketUuid),
			Err:     err,
		}
	}

	return res, nil
}

// DeleteDirectory deletes a directory from a bucket using their UUIDs.
// Returns a DeleteDirectoryResponse struct or an error if the request or unmarshalling fails.
// Handles known error codes for non-existent or already deleted directories.
func DeleteDirectory(ctx context.Context, bucketUuid string, directoryUuid string) (DeleteDirectoryResponse, error) {
	if bucketUuid == "" || directoryUuid == "" {
		return DeleteDirectoryResponse{}, &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "bucket UUID and directory UUID cannot be empty",
		}
	}

	path := "/storage/buckets/" + bucketUuid + "/directories/" + directoryUuid
	res, err := requests.DeleteReq(ctx, path)
	if err != nil {
		return DeleteDirectoryResponse{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to delete directory %s in bucket %s", directoryUuid, bucketUuid),
			Err:     err,
		}
	}

	var resp DeleteDirectoryResponse
	if err := json.Unmarshal([]byte(res), &resp); err != nil {
		return DeleteDirectoryResponse{}, &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to unmarshal delete directory response for directory %s in bucket %s", directoryUuid, bucketUuid),
			Err:     err,
		}
	}

	if resp.Status == ErrCodeDirectoryNotFound {
		return resp, &StorageError{
			Code:    ErrCodeDirectoryNotFound,
			Message: "directory does not exist",
		}
	}
	if resp.Status == ErrCodeDirectoryDeleting {
		return resp, &StorageError{
			Code:    ErrCodeDirectoryDeleting,
			Message: "directory is already marked for deletion",
		}
	}

	return resp, nil
}

// GetOrGenerateIPFSLink retrieves or generates an IPFS link for a given CID.
// Returns the IPFS link as a string, or an error if the request or unmarshalling fails.
func GetOrGenerateIPFSLink(ctx context.Context, cid string) (string, error) {
	if cid == "" {
		return "", &StorageError{
			Code:    ErrCodeInvalidInput,
			Message: "CID cannot be empty",
		}
	}

	path := "/storage/link-on-ipfs/" + cid
	res, err := requests.GetReq(ctx, path, nil)
	if err != nil {
		return "", &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to get IPFS link for CID %s", cid),
			Err:     err,
		}
	}

	var ipfsLinkResponse IPFSLinkResponse
	if err := json.Unmarshal([]byte(res), &ipfsLinkResponse); err != nil {
		return "", &StorageError{
			Code:    500,
			Message: fmt.Sprintf("failed to unmarshal get IPFS link response for CID %s", cid),
			Err:     err,
		}
	}

	if ipfsLinkResponse.Data.Link == "" {
		return "", &StorageError{
			Code:    404,
			Message: fmt.Sprintf("no IPFS link found for CID %s", cid),
		}
	}

	return ipfsLinkResponse.Data.Link, nil
}

// GetIPFSClusterInfo retrieves information about the IPFS cluster.
// Returns an IPFSClusterInfoResponse struct or an error if the request or unmarshalling fails.
func GetIPFSClusterInfo(ctx context.Context) (IPFSClusterInfoResponse, error) {
	path := "/storage/ipfs-cluster-info"
	res, err := requests.GetReq(ctx, path, nil)
	if err != nil {
		return IPFSClusterInfoResponse{}, &StorageError{
			Code:    500,
			Message: "failed to get IPFS cluster info",
			Err:     err,
		}
	}

	var infoResp IPFSClusterInfoResponse
	if err := json.Unmarshal([]byte(res), &infoResp); err != nil {
		return IPFSClusterInfoResponse{}, &StorageError{
			Code:    500,
			Message: "failed to unmarshal IPFS cluster info response",
			Err:     err,
		}
	}

	return infoResp, nil
}
