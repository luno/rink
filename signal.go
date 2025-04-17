package rink

import "time"

// Signal can be used to broadcast a signal to a bunch of goroutines
type Signal struct {
	c chan struct{}
}

// NewSignal returns a Signal ready to be Broadcast
func NewSignal() *Signal {
	return &Signal{
		c: make(chan struct{}),
	}
}

// Broadcast will close the channel C and create a new one for new waiters
func (s *Signal) Broadcast() {
	if s.c != nil {
		close(s.c)
	}
	s.c = make(chan struct{})
}

// Wait returns a channel that will be closed when Broadcast is called
func (s *Signal) Wait() <-chan struct{} {
	return s.c
}

// Immediate returns a channel compatible with time.After that is already ready to trigger
func Immediate() <-chan time.Time {
	ch := make(chan time.Time)
	close(ch)
	return ch
}
