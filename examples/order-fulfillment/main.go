package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/thiagozs/go-saga-engine/engine"
	sagaerrors "github.com/thiagozs/go-saga-engine/errors"
	"github.com/thiagozs/go-saga-engine/event"
	"github.com/thiagozs/go-saga-engine/repository/memory"
	"github.com/thiagozs/go-saga-engine/retry/exponential"
	"github.com/thiagozs/go-saga-engine/state"
)

const orderID = "ORDER-2026-0042"

type LocalBus struct {
	events chan event.Envelope
}

func NewLocalBus(capacity int) *LocalBus {
	return &LocalBus{events: make(chan event.Envelope, capacity)}
}

func (b *LocalBus) Publish(ctx context.Context, envelope event.Envelope) error {
	select {
	case b.events <- envelope:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *LocalBus) Events() <-chan event.Envelope {
	return b.events
}

// FulfillmentServices simula recursos externos. Os mapas também demonstram
// como cada operação pode ser idempotente usando o OrderID como chave.
type FulfillmentServices struct {
	mu sync.Mutex

	reservations     map[string]string
	payments         map[string]string
	shipments        map[string]string
	shippingAttempts map[string]int
	rejectPayment    bool
}

func NewFulfillmentServices(rejectPayment bool) *FulfillmentServices {
	return &FulfillmentServices{
		reservations:     make(map[string]string),
		payments:         make(map[string]string),
		shipments:        make(map[string]string),
		shippingAttempts: make(map[string]int),
		rejectPayment:    rejectPayment,
	}
}

func (s *FulfillmentServices) ReserveInventory(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if reservationID := s.reservations[id]; reservationID != "" {
		return reservationID
	}
	reservationID := "RES-0042"
	s.reservations[id] = reservationID
	return reservationID
}

func (s *FulfillmentServices) ReleaseInventory(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.reservations, id)
}

func (s *FulfillmentServices) Charge(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rejectPayment {
		return "", sagaerrors.Fatal("payment rejected by issuer")
	}
	if paymentID := s.payments[id]; paymentID != "" {
		return paymentID, nil
	}
	paymentID := "PAY-0042"
	s.payments[id] = paymentID
	return paymentID, nil
}

func (s *FulfillmentServices) Refund(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.payments, id)
}

func (s *FulfillmentServices) CreateShipment(id string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if shipmentID := s.shipments[id]; shipmentID != "" {
		return shipmentID, nil
	}
	s.shippingAttempts[id]++
	if s.shippingAttempts[id] == 1 {
		return "", sagaerrors.Retryable("carrier API temporarily unavailable")
	}
	shipmentID := "SHIP-0042"
	s.shipments[id] = shipmentID
	return shipmentID, nil
}

func (s *FulfillmentServices) CancelShipment(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.shipments, id)
}

func (s *FulfillmentServices) Snapshot() (reservation, payment, shipment string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reservations[orderID], s.payments[orderID], s.shipments[orderID]
}

type FulfillmentStage struct {
	name       string
	execute    func(context.Context, *state.State) error
	compensate func(context.Context, *state.State) error
}

func (s *FulfillmentStage) Name() string { return s.name }

func (s *FulfillmentStage) Execute(ctx context.Context, saga *state.State) error {
	slog.Info("executing stage", "stage", s.name, "order_id", saga.Payload["order_id"])
	return s.execute(ctx, saga)
}

func (s *FulfillmentStage) Compensate(ctx context.Context, saga *state.State) error {
	if s.compensate == nil {
		return nil
	}
	slog.Warn("compensating stage", "stage", s.name, "order_id", saga.Payload["order_id"])
	return s.compensate(ctx, saga)
}

func main() {
	rejectPayment := flag.Bool("reject-payment", false, "simulate a fatal payment rejection")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(log)
	if err := run(context.Background(), log, *rejectPayment); err != nil {
		log.Error("order fulfillment example failed", "error", err)
		os.Exit(1)
	}
}

func run(parent context.Context, log *slog.Logger, rejectPayment bool) error {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()

	repo := memory.New()
	bus := NewLocalBus(16)
	services := NewFulfillmentServices(rejectPayment)
	retryPolicy := exponential.New(3, 250*time.Millisecond)
	retryPolicy.MaxDelay = time.Second
	retryPolicy.Jitter = 0

	nodes := fulfillmentNodes(services)
	eng, err := engine.New("order-fulfillment", repo, bus, retryPolicy, log, nodes...)
	if err != nil {
		return fmt.Errorf("create fulfillment engine: %w", err)
	}

	saga, err := eng.Start(ctx, map[string]any{
		"order_id":    orderID,
		"customer_id": "CUSTOMER-1007",
		"sku":         "HEADPHONE-BLACK",
		"quantity":    1,
		"total_cents": 15990,
	})
	if err != nil {
		return fmt.Errorf("start order saga: %w", err)
	}

	if err := consumeUntilFinished(ctx, eng, repo, bus, saga.SagaID, log); err != nil {
		return err
	}

	final, err := repo.Get(ctx, saga.SagaID)
	if err != nil {
		return fmt.Errorf("load final order saga: %w", err)
	}
	printResult(final, services)
	return nil
}

func consumeUntilFinished(
	ctx context.Context,
	eng *engine.Engine,
	repo *memory.Repository,
	bus *LocalBus,
	sagaID string,
	log *slog.Logger,
) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for order saga %q: %w", sagaID, ctx.Err())
		case envelope := <-bus.Events():
			if delay := time.Until(envelope.NotBefore); delay > 0 {
				timer := time.NewTimer(delay)
				select {
				case <-ctx.Done():
					timer.Stop()
					return fmt.Errorf("wait for retry: %w", ctx.Err())
				case <-timer.C:
				}
			}

			if err := eng.HandleNext(ctx, envelope.SagaID); err != nil {
				log.Warn("saga execution returned an error", "error", err)
			}
			current, err := repo.Get(ctx, sagaID)
			if err != nil {
				return fmt.Errorf("inspect order saga: %w", err)
			}
			if current.Status == state.StatusCompleted || current.Status == state.StatusFailed {
				return nil
			}
		}
	}
}

func fulfillmentNodes(services *FulfillmentServices) []engine.Node {
	validate := &FulfillmentStage{name: "validate-order", execute: func(_ context.Context, saga *state.State) error {
		total, ok := saga.Payload["total_cents"].(int)
		if !ok || total <= 0 {
			return sagaerrors.Fatal("order total must be positive")
		}
		saga.Payload["order_validated"] = true
		return nil
	}}

	fraud := &FulfillmentStage{name: "fraud-check", execute: func(_ context.Context, saga *state.State) error {
		saga.Payload["risk_score"] = 12
		saga.Payload["risk_approved"] = true
		return nil
	}}

	reserve := &FulfillmentStage{
		name: "reserve-inventory",
		execute: func(_ context.Context, saga *state.State) error {
			saga.Payload["reservation_id"] = services.ReserveInventory(orderID)
			return nil
		},
		compensate: func(_ context.Context, _ *state.State) error {
			services.ReleaseInventory(orderID)
			return nil
		},
	}

	charge := &FulfillmentStage{
		name: "charge-payment",
		execute: func(_ context.Context, saga *state.State) error {
			paymentID, err := services.Charge(orderID)
			if err != nil {
				return err
			}
			saga.Payload["payment_id"] = paymentID
			return nil
		},
		compensate: func(_ context.Context, _ *state.State) error {
			services.Refund(orderID)
			return nil
		},
	}

	shipment := &FulfillmentStage{
		name: "create-shipment",
		execute: func(_ context.Context, saga *state.State) error {
			shipmentID, err := services.CreateShipment(orderID)
			if err != nil {
				return err
			}
			saga.Payload["shipment_id"] = shipmentID
			return nil
		},
		compensate: func(_ context.Context, _ *state.State) error {
			services.CancelShipment(orderID)
			return nil
		},
	}

	notify := &FulfillmentStage{name: "notify-customer", execute: func(_ context.Context, saga *state.State) error {
		saga.Payload["customer_notified"] = true
		return nil
	}}

	return []engine.Node{
		{Stage: validate},
		{Stage: fraud, DependsOn: []string{"validate-order"}, Parallel: true},
		{Stage: reserve, DependsOn: []string{"validate-order"}, Parallel: true},
		{Stage: charge, DependsOn: []string{"fraud-check", "reserve-inventory"}},
		{Stage: shipment, DependsOn: []string{"charge-payment"}},
		{Stage: notify, DependsOn: []string{"create-shipment"}},
	}
}

func printResult(final *state.State, services *FulfillmentServices) {
	reservation, payment, shipment := services.Snapshot()
	fmt.Println("\n=== ORDER FULFILLMENT RESULT ===")
	fmt.Println("Saga status:", final.Status)
	fmt.Println("Order ID:", final.Payload["order_id"])
	fmt.Println("Reservation:", valueOrNone(reservation))
	fmt.Println("Payment:", valueOrNone(payment))
	fmt.Println("Shipment:", valueOrNone(shipment))
	if final.Error != nil {
		fmt.Printf("Failure: %s: %s\n", final.Error.Stage, final.Error.Message)
	}
	fmt.Println("\nHistory:")
	for _, entry := range final.History {
		fmt.Printf("- %-20s %-20s %s\n", entry.Stage, entry.Status, entry.Message)
	}
}

func valueOrNone(value string) string {
	if value == "" {
		return "<none>"
	}
	return value
}
