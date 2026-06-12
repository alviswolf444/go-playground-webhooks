package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// DeliveryStatus represents the status of a webhook delivery.
type DeliveryStatus string

const (
	StatusProcessing DeliveryStatus = "processing"
	StatusCompleted  DeliveryStatus = "completed"
)

var (
	ErrAlreadyCompleted = errors.New("already completed")
	ErrProcessing       = errors.New("processing")
)

// IdempotencyStore defines the interface for tracking webhook delivery states.
type IdempotencyStore interface {
	TryAcquire(ctx context.Context, deliveryID string, ttl time.Duration) (bool, error)
	SetComplete(ctx context.Context, deliveryID string) error
	Release(ctx context.Context, deliveryID string) error
}

// StoreEntry holds the status and expiration time of a delivery ID.
type StoreEntry struct {
	Status    DeliveryStatus
	ExpiresAt time.Time
}

// InMemoryStore is a thread-safe in-memory implementation of IdempotencyStore.
type InMemoryStore struct {
	mu    sync.Mutex
	store map[string]*StoreEntry
}

// NewInMemoryStore creates a new InMemoryStore and starts a background cleanup goroutine.
func NewInMemoryStore(cleanupInterval time.Duration) *InMemoryStore {
	s := &InMemoryStore{
		store: make(map[string]*StoreEntry),
	}
	go s.cleanupLoop(cleanupInterval)
	return s
}

func (s *InMemoryStore) cleanupLoop(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for k, v := range s.store {
			if now.After(v.ExpiresAt) {
				delete(s.store, k)
			}
		}
		s.mu.Unlock()
	}
}

// TryAcquire attempts to acquire a lock for the delivery ID.
func (s *InMemoryStore) TryAcquire(ctx context.Context, deliveryID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	entry, exists := s.store[deliveryID]
	if exists && now.Before(entry.ExpiresAt) {
		if entry.Status == StatusCompleted {
			return false, ErrAlreadyCompleted
		}
		return false, ErrProcessing
	}

	s.store[deliveryID] = &StoreEntry{
		Status:    StatusProcessing,
		ExpiresAt: now.Add(ttl),
	}
	return true, nil
}

// SetComplete marks the delivery ID as completed.
func (s *InMemoryStore) SetComplete(ctx context.Context, deliveryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry, exists := s.store[deliveryID]
	if !exists {
		return errors.New("delivery ID not found")
	}
	entry.Status = StatusCompleted
	entry.ExpiresAt = time.Now().Add(5 * time.Minute)
	return nil
}

// Release removes the delivery ID from the store.
func (s *InMemoryStore) Release(ctx context.Context, deliveryID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.store, deliveryID)
	return nil
}

// isValidUUID validates if a string is a valid UUID (case-insensitive).
func isValidUUID(u string) bool {
	if len(u) != 36 {
		return false
	}
	for i, r := range u {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
		} else {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') || (r >= 'A' && r <= 'F')) {
				return false
			}
		}
	}
	return true
}

type statusResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusResponseWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}

// WebhookMiddleware wraps a handler with idempotency checks.
func WebhookMiddleware(store IdempotencyStore, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		deliveryID := r.Header.Get("X-GitHub-Delivery")
		if deliveryID == "" {
			http.Error(w, "Missing X-GitHub-Delivery header", http.StatusBadRequest)
			return
		}
		if !isValidUUID(deliveryID) {
			http.Error(w, "Invalid X-GitHub-Delivery header format", http.StatusBadRequest)
			return
		}

		ctx := r.Context()
		ttl := 5 * time.Minute

		acquired, err := store.TryAcquire(ctx, deliveryID, ttl)
		if err != nil {
			if errors.Is(err, ErrAlreadyCompleted) {
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte("Event already processed"))
				return
			}
			if errors.Is(err, ErrProcessing) {
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte("Event is already being processed"))
				return
			}
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		if !acquired {
			w.WriteHeader(http.StatusAccepted)
			w.Write([]byte("Event is already being processed"))
			return
		}

		success := false
		defer func() {
			if !success {
				_ = store.Release(ctx, deliveryID)
			}
		}()

		rec := &statusResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next(rec, r)

		if rec.statusCode >= 200 && rec.statusCode < 300 {
			success = true
			_ = store.SetComplete(ctx, deliveryID)
		}
	}
}

func handleWebhook(w http.ResponseWriter, r *http.Request) {
	log.Printf("Processing event for delivery ID: %s", r.Header.Get("X-GitHub-Delivery"))
	time.Sleep(100 * time.Millisecond)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Webhook processed successfully"))
}

func main() {
	store := NewInMemoryStore(1 * time.Minute)
	http.HandleFunc("/webhook", WebhookMiddleware(store, handleWebhook))

	fmt.Println("Server starting on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
