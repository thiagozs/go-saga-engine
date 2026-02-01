package event

type Next struct {
	SagaID string
}

type Retry struct {
	SagaID string
	Stage  string
}

type DeadLetter struct {
	SagaID string
}
