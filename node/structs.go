package main

type MessageState int64

const (
	Sent MessageState = iota
	Received
)

func (s MessageState) String() string {
	switch s {
	case Sent:
		return "Sent"
	case Received:
		return "Received"
	}
	return "unknown"
}

type RegistrationMessage struct {
	MessageState         MessageState
	ReceivedRegistration *ReceivedRegistration
}

type ReceivedRegistration struct {
	Accepted bool
	Id       string
	Message  string
}
