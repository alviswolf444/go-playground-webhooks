package main

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestIsValidUUID(t *testing.T) {
	tests := []struct {
		uuid  string
		valid bool
	}{
		{"72d3162e-cc78-11e3-814f-4d0898e778a9", true},
		{"72D3162E-CC78-11E3-814F-4D0898E778A9", true},
		{"invalid-uuid", false},
		{"", false},
		{"72d3162e-cc78-11e3-814f-4d0898e778a99", false},
		{"72d3162e-cc78-11e3-814f-4d0898e778a", false},
	}

	for _, tt := range tests {
		if got := isValidUUID(tt.uuid); got != tt.valid {
			t.Errorf("isValidUUID(%q) = %v; want %v", tt.uuid, got, tt.valid)
		}
	}
}

func TestWebhookMiddleware_MissingHeader(t *testing.T) {
	store := NewInMemoryStore(1 * time.Minute)
	handler := WebhookMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("POST", "/webhook", nil)
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestWebhookMiddleware_InvalidHeader(t *testing.T) {
	store := NewInMemoryStore(1 * time.Minute)
	handler := WebhookMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	req := httptest.NewRequest("POST", "/webhook", nil)
	req.Header.Set("X-GitHub-Delivery", "invalid-uuid")
	rec := httptest.NewRecorder()

	handler(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected status 400, got %d", rec.Code)
	}
}

func TestWebhookMiddleware_SuccessAndSubsequent(t *testing.T) {
	store := NewInMemoryStore(1 * time.Minute)
	handler := WebhookMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	deliveryID := "72d3162e-cc78-11e3-814f-4d0898e778a9"

	// First request
	req1 := httptest.NewRequest("POST", "/webhook", nil)
	req1.Header.Set("X-GitHub-Delivery", deliveryID)
	rec1 := httptest.NewRecorder()
	handler(rec1, req1)

	if rec1.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec1.Code)
	}

	// Second request (already completed)
	req2 := httptest.NewRequest("POST", "/webhook", nil)
	req2.Header.Set("X-GitHub-Delivery", deliveryID)
	rec2 := httptest.NewRecorder()
	handler(rec2, req2)

	if rec2.Code != http.StatusAccepted {
		t.Errorf("expected status 202, got %d", rec2.Code)
	}
	if rec2.Body.String() != "Event already processed" {
		t.Errorf("expected body 'Event already processed', got %q", rec2.Body.String())
	}
}

func TestWebhookMiddleware_ConcurrentRequests(t *testing.T) {
	store := NewInMemoryStore(1 * time.Minute)
	
	// We want the handler to block slightly to simulate concurrent processing
	handler := WebhookMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	})

	deliveryID := "72d3162e-cc78-11e3-814f-4d0898e778a9"
	var wg sync.WaitGroup
	wg.Add(2)

	var code1, code2 int
	var body1, body2 string

	go func() {
		defer wg.Done()
		req := httptest.NewRequest("POST", "/webhook", nil)
		req.Header.Set("X-GitHub-Delivery", deliveryID)
		rec := httptest.NewRecorder()
		handler(rec, req)
		code1 = rec.Code
		body1 = rec.Body.String()
	}()

	// Start the second request slightly after the first to ensure the first has acquired the lock
	time.Sleep(20 * time.Millisecond)

	go func() {
		defer wg.Done()
		req := httptest.NewRequest("POST", "/webhook", nil)
		req.Header.Set("X-GitHub-Delivery", deliveryID)
		rec := httptest.NewRecorder()
		handler(rec, req)
		code2 = rec.Code
		body2 = rec.Body.String()
	}()

	wg.Wait()

	// One should be 200 OK, the other should be 202 Accepted
	if (code1 == http.StatusOK && code2 == http.StatusAccepted) || (code1 == http.StatusAccepted && code2 == http.StatusOK) {
		// Success
		if code1 == http.StatusAccepted && body1 != "Event is already being processed" {
			t.Errorf("expected body 'Event is already being processed', got %q", body1)
		}
		if code2 == http.StatusAccepted && body2 != "Event is already being processed" {
			t.Errorf("expected body 'Event is already being processed', got %q", body2)
		}
	} else {
		t.Errorf("unexpected status codes: code1=%d, code2=%d", code1, code2)
	}
}

func TestWebhookMiddleware_FailureReleasesLock(t *testing.T) {
	store := NewInMemoryStore(1 * time.Minute)
	shouldFail := true
	handler := WebhookMiddleware(store, func(w http.ResponseWriter, r *http.Request) {
		if shouldFail {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	deliveryID := "72d3162e-cc78-11e3-814f-4d0898e778a9"

	// First request fails
	req1 := httptest.NewRequest("POST", "/webhook", nil)
	req1.Header.Set("X-GitHub-Delivery", deliveryID)
	rec1 := httptest.NewRecorder()
	handler(rec1, req1)

	if rec1.Code != http.StatusInternalServerError {
		t.Errorf("expected status 500, got %d", rec1.Code)
	}

	// Second request should succeed because the lock was released
	shouldFail = false
	req2 := httptest.NewRequest("POST", "/webhook", nil)
	req2.Header.Set("X-GitHub-Delivery", deliveryID)
	rec2 := httptest.NewRecorder()
	handler(rec2, req2)

	if rec2.Code != http.StatusOK {
		t.Errorf("expected status 200, got %d", rec2.Code)
	}
}
