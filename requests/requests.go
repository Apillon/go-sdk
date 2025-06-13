// Package requests provides helper functions for making authenticated HTTP requests
// to the Apillon API. It supports GET, POST, and DELETE methods, and manages API key authentication.
package requests

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	baseURL     = "https://api.apillon.io"
	maxRetries  = 3
	retryDelay  = time.Second
	timeoutGet  = 30 * time.Second
	timeoutPost = 60 * time.Second
)

var apiKey string

// APIError represents an error response from the Apillon API
type APIError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (status %d): %s", e.Status, e.Message)
}

// SetAPIKey sets the API key to be used for authentication in all requests.
//
// If not set, the package will attempt to read the API key from the APILLON_API_KEY environment variable.
func SetAPIKey(key string) {
	apiKey = key
}

// getAPIKey retrieves the API key for authentication.
// It returns the key set by SetAPIKey, or falls back to the APILLON_API_KEY environment variable.
func getAPIKey() string {
	if apiKey != "" {
		return apiKey
	}
	return os.Getenv("APILLON_API_KEY")
}

// buildURL constructs a URL with query parameters
func buildURL(path string, params map[string]string) (string, error) {
	base, err := url.Parse(baseURL + path)
	if err != nil {
		return "", fmt.Errorf("invalid base URL: %w", err)
	}

	if len(params) > 0 {
		q := base.Query()
		for key, value := range params {
			q.Set(key, value)
		}
		base.RawQuery = q.Encode()
	}

	return base.String(), nil
}

// doRequest performs an HTTP request with retries and proper error handling
func doRequest(ctx context.Context, method, path string, body io.Reader, params map[string]string, timeout time.Duration) (string, error) {
	url, err := buildURL(path, params)
	if err != nil {
		return "", err
	}

	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		req, err := http.NewRequestWithContext(ctx, method, url, body)
		if err != nil {
			return "", fmt.Errorf("failed to create request: %w", err)
		}

		req.Header.Set("Authorization", "Basic "+getAPIKey())
		if method == "POST" {
			req.Header.Set("Content-Type", "application/json")
		}

		client := &http.Client{
			Timeout: timeout,
		}

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(retryDelay * time.Duration(attempt+1))
			continue
		}

		defer resp.Body.Close()

		responseBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return "", fmt.Errorf("failed to read response body: %w", err)
		}

		if resp.StatusCode >= 400 {
			var apiErr APIError
			if err := json.Unmarshal(responseBody, &apiErr); err != nil {
				return "", fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(responseBody))
			}
			return "", &apiErr
		}

		return string(responseBody), nil
	}

	return "", fmt.Errorf("request failed after %d attempts: %w", maxRetries, lastErr)
}

// GetReq sends an authenticated HTTP GET request to the Apillon API.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - path: The API endpoint path (e.g., "/storage/buckets").
//   - params: Optional query parameters as a map[string]string.
//
// Returns:
//   - string: The response body as a string.
//   - error: An error if the request fails or the response cannot be read.
func GetReq(ctx context.Context, path string, params map[string]string) (string, error) {
	return doRequest(ctx, "GET", path, nil, params, timeoutGet)
}

// PostReq sends an authenticated HTTP POST request to the Apillon API.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - path: The API endpoint path (e.g., "/storage/buckets").
//   - body: The request body as an io.Reader (should be JSON).
//
// Returns:
//   - string: The response body as a string.
//   - error: An error if the request fails or the response cannot be read.
func PostReq(ctx context.Context, path string, body io.Reader) (string, error) {
	return doRequest(ctx, "POST", path, body, nil, timeoutPost)
}

// DeleteReq sends an authenticated HTTP DELETE request to the Apillon API.
//
// Parameters:
//   - ctx: Context for cancellation and timeout
//   - path: The API endpoint path (e.g., "/storage/buckets/{uuid}").
//
// Returns:
//   - string: The response body as a string.
//   - error: An error if the request fails or the response cannot be read.
func DeleteReq(ctx context.Context, path string) (string, error) {
	return doRequest(ctx, "DELETE", path, nil, nil, timeoutGet)
}
