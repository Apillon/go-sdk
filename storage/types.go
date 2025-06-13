package storage

// FileMetadata represents metadata for a file, including its name and content type.
type FileMetadata struct {
	FileName    string `json:"fileName" validate:"required"` // Name of the file
	ContentType string `json:"contentType"`                  // MIME type of the file
}

// WholeFile represents a file's content and its associated metadata.
type WholeFile struct {
	Content  string       `json:"content"`  // File content, typically base64-encoded
	Metadata FileMetadata `json:"metadata"` // Metadata about the file
}

// FileItem represents a file entry, including its path, name, type, URL, and UUID.
type FileItem struct {
	Path        *string `json:"path"`        // Path to the file (nullable)
	FileName    string  `json:"fileName"`    // Name of the file
	ContentType string  `json:"contentType"` // MIME type of the file
	URL         string  `json:"url"`         // URL to access the file
	FileUUID    string  `json:"fileUuid"`    // Unique identifier for the file
}

// ProcessData represents the data object in the ProcessAPIResponse, including session UUID and files.
type ProcessData struct {
	SessionUUID string     `json:"sessionUuid"` // Unique identifier for the session
	Files       []FileItem `json:"files"`       // List of files processed in the session
}

// APIResponse is a generic API response wrapper for all endpoints.
// T is the type of the Data field.
type APIResponse[T any] struct {
	ID     string `json:"id"`     // Unique identifier for the response
	Status int    `json:"status"` // Status code of the response
	Data   T      `json:"data"`   // Response data of generic type T
}

// ListBucketsResponse represents a response containing a list of buckets.
type ListBucketsResponse = APIResponse[BucketListData]

// ListFilesResponse represents a response containing a list of files.
type ListFilesResponse = APIResponse[FileListData]

// ProcessAPIResponse represents a response for file processing operations.
type ProcessAPIResponse = APIResponse[ProcessData]

// FileDetails represents a response containing details about a specific file.
type FileDetails = APIResponse[FileInfo]

// DeleteDirectoryResponse represents a response for directory deletion operations.
type DeleteDirectoryResponse = APIResponse[bool]

// IPFSClusterInfoResponse represents a response containing IPFS cluster information.
type IPFSClusterInfoResponse = APIResponse[IPFSClusterInfoData]

// IPFSClusterInfoData contains information about the IPFS cluster.
type IPFSClusterInfoData struct {
	Secret      string `json:"secret"`       // Secret key for the IPFS cluster
	ProjectUUID string `json:"project_uuid"` // UUID of the associated project
	IPFSGateway string `json:"ipfsGateway"`  // IPFS gateway URL
	IPNSGateway string `json:"ipnsGateway"`  // IPNS gateway URL
}

// IPFSLinkResponse represents a response containing an IPFS link.
type IPFSLinkResponse = APIResponse[struct {
	Link string `json:"link"` // IPFS link for the requested CID
}]

// ListData is a generic structure for paginated lists.
// T is the type of the items in the list.
type ListData[T any] struct {
	Items []T `json:"items"` // List of items
	Total int `json:"total"` // Total number of items available
}

// BucketListData represents a paginated list of buckets.
type BucketListData = ListData[BucketItem]

// FileListData represents a paginated list of files.
type FileListData = ListData[FileInfo]

// Timestamps contains common timestamp fields for created and updated times.
type Timestamps struct {
	CreateTime string `json:"createTime"` // Creation timestamp (ISO8601 format)
	UpdateTime string `json:"updateTime"` // Last update timestamp (ISO8601 format)
}

// FileInfo contains detailed information about a file.
type FileInfo struct {
	Timestamps
	FileUUID      string  `json:"fileUuid"`                // Unique identifier for the file
	CID           string  `json:"CID"`                     // Content Identifier (CID) for IPFS
	Name          string  `json:"name"`                    // Name of the file
	ContentType   string  `json:"contentType"`             // MIME type of the file
	Path          *string `json:"path"`                    // Path to the file (nullable)
	Size          int64   `json:"size"`                    // Size of the file in bytes
	FileStatus    int     `json:"fileStatus"`              // Status code of the file
	Link          string  `json:"link"`                    // URL or IPFS link to the file
	DirectoryUUID *string `json:"directoryUuid,omitempty"` // UUID of the parent directory (nullable)
}

// BucketItem contains information about a storage bucket.
type BucketItem struct {
	Timestamps
	BucketUUID  string `json:"bucketUuid"`  // Unique identifier for the bucket
	BucketType  int    `json:"bucketType"`  // Type of the bucket
	Name        string `json:"name"`        // Name of the bucket
	Description string `json:"description"` // Description of the bucket
	Size        int64  `json:"size"`        // Total size of the bucket in bytes
}

type startUploadRequest struct {
	Files []FileMetadata `json:"files"`
}
