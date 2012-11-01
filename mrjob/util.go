package mrjob

import (
	"reflect"
)

// Pair represents a key-value pair to be processed in a MapReduce job. Keys
// are compared using reflect.DeepEqual, or if the Key implements Equaler, that
// takes precedence.
type Pair struct {
	Key, Value interface{}
}

func pairKey(x interface{}) interface{} {
	p := x.(*Pair)
	return p.Key
}

// Equaler allows Keys to implement a custom equality operator.
type Equaler interface {
	Equal(x interface{}) bool
}

func equals(x, y interface{}) bool {
	switch a := x.(type) {
	case Equaler:
		return a.Equal(y)
	}
	return reflect.DeepEqual(x, y)
}

// Group stores a Key and the values grouped under that key.
type Group struct {
	Key    interface{}
	Values chan interface{}
}

type keyFunc func(x interface{}) interface{}

// GroupBy groups contiguous elements from a channel by comparing them with
// the key function. Groups are lazily constructed and sent to the output
// channel.
func GroupBy(c chan interface{}, key keyFunc) (out chan *Group) {
	out = make(chan *Group)

	go func() {
		current := &Group{nil, make(chan interface{})}
		for x := range c {
			k := key(x)
			// If we're on the first group, set the key, and send it along.
			if current.Key == nil {
				current.Key = k
				out <- current
			// If the key changed, then close the last group, create a new
			// one, and send it over.
			} else if !equals(current.Key, k) {
				close(current.Values)
				current = &Group{k, make(chan interface{})}
				out <- current
			}

			// Either way, send the current item to the group.
			current.Values <- x
		}
		close(current.Values)
		close(out)
	}()
	return out
}
