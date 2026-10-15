package state

import (
	"reflect"
	"time"
)

type Status string

const (
	StatusPending      Status = "PENDING"
	StatusRunning      Status = "RUNNING"
	StatusWaitingRetry Status = "WAITING_RETRY"
	StatusCompleted    Status = "COMPLETED"
	StatusFailed       Status = "FAILED"
	StatusCompensated  Status = "COMPENSATED"
)

type State struct {
	SagaID       string
	Name         string
	Status       Status
	CurrentStage string
	Version      uint64

	Payload map[string]any

	ExecutedStages  map[string]bool
	CompletedStages []string
	Attempts        map[string]int
	History         []HistoryEntry

	Retry *RetryInfo
	Error *ErrorInfo

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Clone returns an isolated engine state. Payload values composed of maps,
// slices and pointers are recursively copied by CloneValue.
func Clone(s *State) *State {
	if s == nil {
		return nil
	}
	c := *s
	c.Payload = ClonePayload(s.Payload)
	c.ExecutedStages = make(map[string]bool, len(s.ExecutedStages))
	for name, executed := range s.ExecutedStages {
		c.ExecutedStages[name] = executed
	}
	c.CompletedStages = append([]string(nil), s.CompletedStages...)
	c.Attempts = make(map[string]int, len(s.Attempts))
	for name, attempts := range s.Attempts {
		c.Attempts[name] = attempts
	}
	c.History = append([]HistoryEntry(nil), s.History...)
	if s.Retry != nil {
		retryCopy := *s.Retry
		c.Retry = &retryCopy
	}
	if s.Error != nil {
		errorCopy := *s.Error
		c.Error = &errorCopy
	}
	return &c
}

type RetryInfo struct {
	Stage     string
	Attempt   int
	NotBefore time.Time
	LastError string
}

func ClonePayload(payload map[string]any) map[string]any {
	if payload == nil {
		return make(map[string]any)
	}
	cloned := make(map[string]any, len(payload))
	for key, value := range payload {
		cloned[key] = CloneValue(value)
	}
	return cloned
}

// CloneValue deeply copies the map/slice/pointer shapes commonly used in saga
// payloads. Immutable and opaque values are copied by value.
func CloneValue(value any) any {
	if value == nil {
		return nil
	}
	return cloneValue(reflect.ValueOf(value)).Interface()
}

func cloneValue(value reflect.Value) reflect.Value {
	if !value.IsValid() {
		return value
	}
	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		cloned := cloneValue(value.Elem())
		out := reflect.New(value.Type()).Elem()
		out.Set(cloned)
		return out
	case reflect.Map:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeMapWithSize(value.Type(), value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			out.SetMapIndex(iterator.Key(), cloneValue(iterator.Value()))
		}
		return out
	case reflect.Slice:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.MakeSlice(value.Type(), value.Len(), value.Len())
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneValue(value.Index(i)))
		}
		return out
	case reflect.Array:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.Len(); i++ {
			out.Index(i).Set(cloneValue(value.Index(i)))
		}
		return out
	case reflect.Pointer:
		if value.IsNil() {
			return reflect.Zero(value.Type())
		}
		out := reflect.New(value.Type().Elem())
		out.Elem().Set(cloneValue(value.Elem()))
		return out
	case reflect.Struct:
		out := reflect.New(value.Type()).Elem()
		for i := 0; i < value.NumField(); i++ {
			if !value.Field(i).CanInterface() {
				return value
			}
			out.Field(i).Set(cloneValue(value.Field(i)))
		}
		return out
	default:
		return value
	}
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
