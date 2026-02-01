package state

import "time"

type Status string

const (
	StatusPending     Status = "PENDING"
	StatusRunning     Status = "RUNNING"
	StatusCompleted   Status = "COMPLETED"
	StatusFailed      Status = "FAILED"
	StatusCompensated Status = "COMPENSATED"
)

type State struct {
	SagaID       string
	Name         string
	Status       Status
	CurrentStage string

	Payload map[string]any

	ExecutedStages map[string]bool
	History        []HistoryEntry

	Error *ErrorInfo

	CreatedAt time.Time
	UpdatedAt time.Time
}

type ErrorInfo struct {
	Stage   string
	Message string
}

type HistoryEntry struct {
	Stage     string
	Status    string
	Message   string
	Timestamp time.Time
}
