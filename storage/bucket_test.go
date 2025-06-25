package storage

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestCreateAndGetBucket(t *testing.T) {
	// Test bucket creation
	bucketName := "test-bucket-" + time.Now().Format("20060102150405")
	description := "Test bucket for unit testing"

	t.Run("CreateBucket", func(t *testing.T) {
		err := CreateBucket(context.Background(), bucketName, description)
		if err != nil {
			t.Errorf("CreateBucket failed: %v", err)
		}
	})

	// Give some time for the bucket to be created
	time.Sleep(2 * time.Second)

	t.Run("GetBucket", func(t *testing.T) {
		result, err := GetBucket(context.Background(), bucketName)
		if err != nil {
			t.Errorf("GetBucket failed: %v", err)
		}

		if result.Data.Total == 0 {
			t.Error("GetBucket returned empty result")
		}
		t.Logf("Bucket details: %+v", result)
	})
}

func TestGetBucketWithEmptyName(t *testing.T) {
	result, err := GetBucket(context.Background(), "")
	if err != nil {
		t.Errorf("GetBucket with empty name failed: %v", err)
	}

	if result.Data.Total != 0 {
		// If the API returns buckets even with an empty name, this is unexpected
		t.Logf("GetBucket with empty name returned empty result")
	}
	t.Logf("All buckets: %+v", result)
}

func TestCreateBucketWithoutDescription(t *testing.T) {
	bucketName := "test-bucket-no-desc-" + time.Now().Format("20060102150405")

	err := CreateBucket(context.Background(), bucketName, "")
	if err != nil {
		t.Errorf("CreateBucket without description failed: %v", err)
	}
}

func TestUploadFileProcess(t *testing.T) {
	// First create a test bucket
	bucketName := "test-upload-bucket-" + time.Now().Format("20060102150405")
	description := "Test bucket for file upload testing"

	err := CreateBucket(context.Background(), bucketName, description)
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Give some time for bucket creation
	time.Sleep(2 * time.Second)

	// Get the bucket to extract the UUID
	bucketRes, err := GetBucket(context.Background(), bucketName)
	if err != nil {
		t.Fatalf("Failed to get bucket details: %v", err)
	}

	// You'll need to parse the bucket UUID from the response
	// This is a simplified version - you may need to adjust based on your actual response format
	bucketUuid, err := extractBucketUuid(bucketRes, bucketName, t)
	if err != nil {
		t.Fatalf("Failed to extract bucket UUID: %v", err)
	}

	content := "0x04eece2df61aed2057a41ffc8b9af72651a1db02a51d3bd7ff6f5a83174722156bd10d3cbbd9d78cc0842564bc8b073d32fb0ecf1c932267711c9c57726c8068f8dedadcffcf793ab80cb7760c62b1967a0485c6c4b3ba760d7610c361820c4d3bfbb0b33932fb88f3fcf11788d1dd3022b64209db9681e0400df077c2aede5417ceaabd6df052c845945edb0978ced629cf05f313b0b6dcf7a0496b28ac6abec89f20027c0daad3f8410f3e4b801fc0c0c3cca24f05bf690c81257ebbfbf4b741287ca4c21e9ed50bcea74930b2b74d9737e909022fed959d0c60928a198c64cd7553cdd03091cd80cbab0f091afa1e46816057b22fbde336f4cc6eb123c87d2f90ab324b7a5150d9a0fd"

	t.Run("UploadSingleFile", func(t *testing.T) {
		files := []WholeFile{
			{
				Metadata: FileMetadata{
					FileName:    "test-file.txt",
					ContentType: "text/plain",
				},
				Content: content, // Use the content defined above
			},
		}

		result, err := UploadFileProcess(context.Background(), bucketUuid, files)
		if err != nil {
			t.Errorf("UploadFileProcess failed for single file: %v", err)
		}
		if result == "" {
			t.Error("UploadFileProcess returned empty result for single file")
		}
		t.Logf("Single file upload result: %s", result)
	})

	// t.Run("UploadMultipleFiles", func(t *testing.T) {
	// 	files := []WholeFile{
	// 		{
	// 			Metadata: FileMetadata{
	// 				FileName:    "test-file-1.txt",
	// 				ContentType: "text/plain",
	// 			},
	// 			Content: "Content of first test file.",
	// 		},
	// 		{
	// 			Metadata: FileMetadata{
	// 				FileName:    "test-file-2.json",
	// 				ContentType: "application/json",
	// 			},
	// 			Content: `{"message": "This is a test JSON file", "timestamp": "2025-06-08"}`,
	// 		},
	// 	}

	// 	result, err := UploadFileProcess(bucketUuid, files)
	// 	if err != nil {
	// 		t.Errorf("UploadFileProcess failed for multiple files: %v", err)
	// 	}
	// 	if result == "" {
	// 		t.Error("UploadFileProcess returned empty result for multiple files")
	// 	}
	// 	t.Logf("Multiple files upload result: %s", result)
	// })

	// t.Run("UploadEmptyFile", func(t *testing.T) {
	// 	files := []WholeFile{
	// 		{
	// 			Metadata: FileMetadata{
	// 				FileName:    "empty-file.txt",
	// 				ContentType: "text/plain",
	// 			},
	// 			Content: "", // Empty content should cause error
	// 		},
	// 	}

	// 	result, err := UploadFileProcess(bucketUuid, files)
	// 	if err == nil {
	// 		t.Error("UploadFileProcess should have failed for empty file content")
	// 	}
	// 	if result != "" {
	// 		t.Error("UploadFileProcess should return empty result on error")
	// 	}
	// 	t.Logf("Expected error for empty file: %v", err)
	// })

	// t.Run("UploadWithInvalidBucketUuid", func(t *testing.T) {
	// 	files := []WholeFile{
	// 		{
	// 			Metadata: FileMetadata{
	// 				FileName:    "test-file.txt",
	// 				ContentType: "text/plain",
	// 			},
	// 			Content: "Test content",
	// 		},
	// 	}

	// 	invalidUuid := "invalid-bucket-uuid-12345"
	// 	result, err := UploadFileProcess(invalidUuid, files)
	// 	if err == nil {
	// 		t.Error("UploadFileProcess should have failed for invalid bucket UUID")
	// 	}
	// 	if result != "" {
	// 		t.Error("UploadFileProcess should return empty result on error")
	// 	}
	// 	t.Logf("Expected error for invalid bucket UUID: %v", err)
	// })

	// t.Run("UploadNoFiles", func(t *testing.T) {
	// 	files := []WholeFile{}

	// 	result, err := UploadFileProcess(bucketUuid, files)
	// 	if err == nil {
	// 		t.Error("UploadFileProcess should have failed for empty files array")
	// 	}
	// 	if result != "" {
	// 		t.Error("UploadFileProcess should return empty result on error")
	// 	}
	// 	t.Logf("Expected error for no files: %v", err)
	// })
}

// Helper function to extract bucket UUID from the response
// You'll need to implement this based on your actual API response format
func extractBucketUuid(bucketResponse ListBucketsResponse, bucketName string, t *testing.T) (string, error) {
	for _, item := range bucketResponse.Data.Items {
		if item.Name == bucketName {
			return item.BucketUUID, nil
		}
	}

	return "", nil
}

func TestUploadFileProcessEdgeCases(t *testing.T) {
	t.Run("LargeFileUpload", func(t *testing.T) {
		// Test with a larger file
		largeContent := strings.Repeat("This is a large file content. ", 1000)

		files := []WholeFile{
			{
				Metadata: FileMetadata{
					FileName:    "large-file.txt",
					ContentType: "text/plain",
				},
				Content: largeContent,
			},
		}

		// You'll need a valid bucket UUID for this test
		bucketUuid := "your-test-bucket-uuid"

		result, err := UploadFileProcess(context.Background(), bucketUuid, files)
		if err != nil {
			t.Logf("Large file upload failed (this might be expected): %v", err)
		} else {
			t.Logf("Large file upload succeeded: %s", result)
		}
	})

	t.Run("SpecialCharactersInFilename", func(t *testing.T) {
		files := []WholeFile{
			{
				Metadata: FileMetadata{
					FileName:    "file-with-special-chars-åæø.txt",
					ContentType: "text/plain",
				},
				Content: "Content with special characters: åæøÅÆØ",
			},
		}

		bucketUuid := "your-test-bucket-uuid"

		result, err := UploadFileProcess(context.Background(), bucketUuid, files)
		if err != nil {
			t.Logf("Special characters filename upload failed: %v", err)
		} else {
			t.Logf("Special characters filename upload succeeded: %s", result)
		}
	})
}

func TestGetSpecificBucketByName(t *testing.T) {
	bucketName := "test-bucket-by-name-" + time.Now().Format("20060102150405")

	err := CreateBucket(context.Background(), bucketName, "Test bucket for get specific bucket by name")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Test getting the specific bucket by name
	result, err := GetBucket(context.Background(), bucketName)
	if err != nil {
		t.Errorf("GetBucket failed for specific bucket name '%s': %v", bucketName, err)
		return
	}

	// Verify we got the correct bucket
	if result.Data.Total == 0 {
		t.Errorf("No buckets found with name '%s'", bucketName)
		return
	}

	// Find our specific bucket in the response
	found := false
	var foundBucket BucketItem
	for _, item := range result.Data.Items {
		if item.Name == bucketName {
			found = true
			foundBucket = item
			break
		}
	}

	if !found {
		t.Errorf("Bucket with name '%s' not found in response", bucketName)
		return
	}

	// Verify bucket properties
	if foundBucket.Name != bucketName {
		t.Errorf("Expected bucket name '%s', got '%s'", bucketName, foundBucket.Name)
	}

	if foundBucket.BucketUUID == "" {
		t.Error("Bucket UUID should not be empty")
	}

	t.Logf("Successfully found bucket: Name='%s', UUID='%s', Description='%s'",
		foundBucket.Name, foundBucket.BucketUUID, foundBucket.Description)
}

func TestUploadFileToExistingBucketByName(t *testing.T) {
	// Create a test bucket first
	bucketName := "test-bucket-by-name-" + time.Now().Format("20060102150405")

	err := CreateBucket(context.Background(), bucketName, "Test bucket for upload file to existing bucket by name")
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Get the bucket to extract its UUID
	bucketRes, err := GetBucket(context.Background(), bucketName)
	if err != nil {
		t.Fatalf("Failed to get bucket details for '%s': %v", bucketName, err)
	}

	// Extract bucket UUID
	bucketUuid, err := extractBucketUuid(bucketRes, bucketName, t)
	if err != nil || bucketUuid == "" {
		t.Fatalf("Failed to extract bucket UUID for bucket '%s': %v", bucketName, err)
	}

	t.Logf("Found bucket '%s' with UUID: %s", bucketName, bucketUuid)

	// Prepare test files for upload
	testFiles := []WholeFile{
		{
			Metadata: FileMetadata{
				FileName:    "document.txt",
				ContentType: "text/plain",
			},
			Content: "This is a test document uploaded to bucket: " + bucketName,
		},
		{
			Metadata: FileMetadata{
				FileName:    "data.json",
				ContentType: "application/json",
			},
			Content: `{
                "bucket": "` + bucketName + `",
                "uploadTime": "` + time.Now().Format(time.RFC3339) + `",
                "testData": "This is test JSON data"
            }`,
		},
	}

	// Upload files to the existing bucket
	result, err := UploadFileProcess(context.Background(), bucketUuid, testFiles)
	if err != nil {
		t.Errorf("Failed to upload files to existing bucket '%s' (UUID: %s): %v",
			bucketName, bucketUuid, err)
		return
	}

	if result == "" {
		t.Error("UploadFileProcess returned empty result")
		return
	}

	t.Logf("Successfully uploaded %d files to bucket '%s': %s",
		len(testFiles), bucketName, result)

	// Verify the upload was successful by checking the result
	// (You might want to add additional verification here based on your API response format)
	if !strings.Contains(result, "success") && !strings.Contains(result, "completed") {
		t.Logf("Upload result may indicate issues: %s", result)
	}
}

// Helper function to upload files to a bucket by name (convenience wrapper)
func UploadFilesToBucketByName(bucketName string, files []WholeFile, t *testing.T) (string, error) {
	// Get bucket details by name
	bucketRes, err := GetBucket(context.Background(), bucketName)
	if err != nil {
		return "", fmt.Errorf("failed to get bucket '%s': %w", bucketName, err)
	}

	// Extract bucket UUID
	bucketUuid, err := extractBucketUuid(bucketRes, bucketName, t)
	if err != nil || bucketUuid == "" {
		return "", fmt.Errorf("failed to extract UUID for bucket '%s': %w", bucketName, err)
	}

	// Upload files using the UUID
	return UploadFileProcess(context.Background(), bucketUuid, files)
}

func TestUploadFileToExistingBucketByNameWithHelper(t *testing.T) {
	// This test demonstrates using the helper function
	bucketName := "helper-test-bucket-" + time.Now().Format("20060102150405")
	description := "Test bucket for helper function"

	// Create bucket
	err := CreateBucket(context.Background(), bucketName, description)
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	time.Sleep(2 * time.Second)

	// Prepare a single test file
	testFile := []WholeFile{
		{
			Metadata: FileMetadata{
				FileName:    "helper-test.txt",
				ContentType: "text/plain",
			},
			Content: "This file was uploaded using the helper function to bucket: " + bucketName,
		},
	}

	// Upload using the helper function
	result, err := UploadFilesToBucketByName(bucketName, testFile, t)
	if err != nil {
		t.Errorf("Helper function failed to upload to bucket '%s': %v", bucketName, err)
		return
	}

	t.Logf("Helper function successfully uploaded file to bucket '%s': %s", bucketName, result)
}

func TestEndSessionManual(t *testing.T) {
	// Replace these with actual values
	bucketUuid := ""
	sessionId := ""

	if bucketUuid == "" || sessionId == "" {
		t.Errorf("Please manually create a bucket and session to test this function")
		return
	}

	result, err := EndSession(context.Background(), bucketUuid, sessionId)
	if err != nil {
		t.Errorf("EndSession failed: %v", err)
		return
	}

	t.Logf("EndSession result: %s", result)
}

func TestCompleteFileLifecycle(t *testing.T) {
	// Step 1: Create a test bucket
	bucketName := "lifecycle-test-bucket-" + time.Now().Format("20060102150405")
	description := "Test bucket for complete file lifecycle testing"

	err := CreateBucket(context.Background(), bucketName, description)
	if err != nil {
		t.Fatalf("Failed to create test bucket: %v", err)
	}

	// Give some time for bucket creation
	time.Sleep(2 * time.Second)

	// Step 2: Get the bucket to extract the UUID
	bucketRes, err := GetBucket(context.Background(), bucketName)
	if err != nil {
		t.Fatalf("Failed to get bucket details: %v", err)
	}

	bucketUuid, err := extractBucketUuid(bucketRes, bucketName, t)
	if err != nil || bucketUuid == "" {
		t.Fatalf("Failed to extract bucket UUID: %v", err)
	}

	t.Logf("Created bucket '%s' with UUID: %s", bucketName, bucketUuid)

	// Step 3: Prepare test files for upload
	testFiles := []WholeFile{
		{
			Metadata: FileMetadata{
				FileName:    "lifecycle-test-document.txt",
				ContentType: "text/plain",
			},
			Content: "This is a test document for the complete lifecycle test.",
		},
		{
			Metadata: FileMetadata{
				FileName:    "lifecycle-test-config.json",
				ContentType: "application/json",
			},
			Content: `{
                "testName": "Complete File Lifecycle",
                "bucket": "` + bucketName + `",
                "uploadTime": "` + time.Now().Format(time.RFC3339) + `",
                "fileCount": 2
            }`,
		},
	}

	// Step 4: Upload files to the bucket
	t.Run("UploadFiles", func(t *testing.T) {
		result, err := UploadFileProcess(context.Background(), bucketUuid, testFiles)
		if err != nil {
			t.Fatalf("Failed to upload files to bucket '%s': %v", bucketName, err)
		}

		if result == "" {
			t.Fatal("UploadFileProcess returned empty result")
		}

		t.Logf("Successfully uploaded %d files to bucket '%s': %s", len(testFiles), bucketName, result)
	})

	// Give some time for files to be processed
	time.Sleep(3 * time.Second)

	// Step 5: List files in the bucket to get file UUIDs
	t.Run("ListFilesAndGetDetails", func(t *testing.T) {
		fileList, err := ListFilesInBucket(context.Background(), bucketUuid)
		if err != nil {
			t.Fatalf("Failed to list files in bucket '%s': %v", bucketName, err)
		}

		if fileList.Data.Total == 0 {
			t.Fatal("No files found in bucket after upload")
		}

		t.Logf("Found %d files in bucket '%s'", fileList.Data.Total, bucketName)

		// Step 6: Get details for each uploaded file
		for i, fileInfo := range fileList.Data.Items {
			t.Run(fmt.Sprintf("GetFileDetails_%d", i+1), func(t *testing.T) {
				fileDetails, err := GetFileDetails(context.Background(), bucketUuid, fileInfo.FileUUID)
				if err != nil {
					t.Errorf("Failed to get details for file '%s' (UUID: %s): %v",
						fileInfo.Name, fileInfo.FileUUID, err)
					return
				}

				// Verify file details
				if fileDetails.Data.FileUUID != fileInfo.FileUUID {
					t.Errorf("File UUID mismatch: expected '%s', got '%s'",
						fileInfo.FileUUID, fileDetails.Data.FileUUID)
				}

				if fileDetails.Data.Name == "" {
					t.Error("File name should not be empty")
				}

				if fileDetails.Data.Size <= 0 {
					t.Error("File size should be greater than 0")
				}

				// Check if the file name matches one of our uploaded files
				foundMatch := false
				for _, uploadedFile := range testFiles {
					if fileDetails.Data.Name == uploadedFile.Metadata.FileName {
						foundMatch = true
						if fileDetails.Data.ContentType != uploadedFile.Metadata.ContentType {
							t.Errorf("Content type mismatch for file '%s': expected '%s', got '%s'",
								fileDetails.Data.Name, uploadedFile.Metadata.ContentType, fileDetails.Data.ContentType)
						}
						break
					}
				}

				if !foundMatch {
					t.Errorf("Uploaded file '%s' not found in expected files list", fileDetails.Data.Name)
				}

				t.Logf("File Details - Name: '%s', UUID: '%s', Size: %d bytes, ContentType: '%s', CID: '%s'",
					fileDetails.Data.Name, fileDetails.Data.FileUUID, fileDetails.Data.Size,
					fileDetails.Data.ContentType, fileDetails.Data.CID)
			})
		}

		// Step 7: Verify all uploaded files were found
		if len(fileList.Data.Items) != len(testFiles) {
			t.Errorf("Expected %d files in bucket, found %d", len(testFiles), len(fileList.Data.Items))
		}
	})

	// Step 8: Get IPFS links for each file
	t.Run("GetIPFSLinks", func(t *testing.T) {
		t.Logf("Waiting for files to be processed and IPFS links to be generated...")
		time.Sleep(30 * time.Second) // Wait for file processing to complete
		t.Logf("Starting IPFS link retrieval for bucket '%s'", bucketName)

		fileList, err := ListFilesInBucket(context.Background(), bucketUuid)
		if err != nil {
			t.Fatalf("Failed to list files in bucket '%s': %v", bucketName, err)
		}

		if fileList.Data.Total == 0 {
			t.Fatal("No files found in bucket to get IPFS links")
		}

		t.Logf("Found %d files in bucket '%s' for IPFS link retrieval", fileList.Data.Total, bucketName)

		for _, fileInfo := range fileList.Data.Items {
			t.Run(fmt.Sprintf("GetIPFSLink_%s", fileInfo.FileUUID), func(t *testing.T) {
				if fileInfo.CID == "" {
					t.Errorf("File '%s' (UUID: %s) does not have a CID, cannot get IPFS link",
						fileInfo.Name, fileInfo.FileUUID)
					return
				}
				ipfsLink, err := GetOrGenerateIPFSLink(context.Background(), fileInfo.CID)
				if err != nil {
					t.Errorf("Failed to get IPFS link for file '%s' (UUID: %s, CID: %s): %v",
						fileInfo.Name, fileInfo.FileUUID, fileInfo.CID, err)
					return
				}
				if ipfsLink == "" {
					t.Error("IPFS link should not be empty")
					return
				}

				t.Logf("IPFS link for file '%s' (UUID: %s, CID: %s): %s",
					fileInfo.Name, fileInfo.FileUUID, fileInfo.CID, ipfsLink)
			})
		}
	})

	// Step 9: Optional - Test bucket content retrieval
	t.Run("GetBucketContent", func(t *testing.T) {
		content, err := GetBucketContent(context.Background(), bucketUuid)
		if err != nil {
			t.Errorf("Failed to get bucket content: %v", err)
			return
		}

		if content == "" {
			t.Error("Bucket content should not be empty")
			return
		}

		t.Logf("Bucket content retrieved successfully, length: %d characters", len(content))
	})
}

func TestGetFileDetails(t *testing.T) {
	// Replace these with actual values
	bucketUuid := ""
	fileUuid := ""

	if bucketUuid == "" || fileUuid == "" {
		t.Errorf("Please manually create a bucket and file to test this function")
		return
	}

	fileDetails, err := GetFileDetails(context.Background(), bucketUuid, fileUuid)
	if err != nil {
		t.Errorf("GetFileDetails failed: %v", err)
		return
	}

	if fileDetails.Data.FileUUID == "" {
		t.Error("File UUID should not be empty")
		return
	}

	if fileDetails.Data.Name == "" {
		t.Error("File name should not be empty")
		return
	}

	t.Logf("File details: %+v", fileDetails)
}
