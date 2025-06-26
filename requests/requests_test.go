package requests

import "testing"

func TestCheckAPIKey(t *testing.T) {
	apiKey := getAPIKey()

	t.Logf("API key: %s", apiKey)

	if apiKey == "" {
		t.Errorf("API key not set correctly")
	}
}
