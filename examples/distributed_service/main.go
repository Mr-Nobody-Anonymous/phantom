package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// OrderService represents an instrumented networked service in Go
// that processes client orders and demonstrates concurrency anomalies.
type Order struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"`
	Charged   bool      `json:"charged"`
	CreatedAt time.Time `json:"created_at"`
}

var (
	orderStore = make(map[string]*Order)
	storeMu    sync.Mutex
)

func handleOrder(w http.ResponseWriter, r *http.Request) {
	orderID := r.URL.Query().Get("id")
	if orderID == "" {
		orderID = "ord_default"
	}

	// 1. Read phase (simulates concurrent non-atomic read)
	storeMu.Lock()
	ord, exists := orderStore[orderID]
	if !exists {
		ord = &Order{ID: orderID, Status: "pending", Charged: false, CreatedAt: time.Now()}
		orderStore[orderID] = ord
	}
	currentStatus := ord.Status
	storeMu.Unlock()

	// 2. Simulated network latency / timing perturbation
	time.Sleep(50 * time.Millisecond)

	// 3. Double-charge race condition: both concurrent requests observe "pending"
	storeMu.Lock()
	if currentStatus == "pending" {
		if ord.Charged {
			// Bug triggered: duplicate charge!
			w.WriteHeader(http.StatusConflict)
			json.NewEncoder(w).Encode(map[string]string{
				"error":     "DuplicateOrderProcessing",
				"order_id":  orderID,
				"invariant": "NoDuplicateProcessingViolated",
			})
			storeMu.Unlock()
			return
		}
		ord.Charged = true
		ord.Status = "completed"
	}
	storeMu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(ord)
}

func main() {
	http.HandleFunc("/order", handleOrder)
	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"status":"UP"}`)
	})

	log.Println("Project PHANTOM Go Instrumented Order Service listening on :8081...")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
