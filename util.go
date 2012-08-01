package main

type Pair struct {
	Key, Value interface{}
}

func pairKey(x interface{}) interface{} {
	p := x.(*Pair)
	return p.Key
}

type Equaler interface {
	Equal(x interface{}) bool
}

func equals(x, y interface{}) bool {
	switch a := x.(type) {
	case Equaler:
		return a.Equal(y)
	}
	return x == y
}

type Group struct {
	Key    interface{}
	Values chan interface{}
}

type keyFunc func(x interface{}) interface{}
type equalFunc func(x, y interface{}) bool

func GroupBy(c chan interface{}, key keyFunc) (out chan *Group) {
	out = make(chan *Group)

	go func() {
		current := &Group{nil, make(chan interface{})}
		for x := range c {
			k := key(x)
			logger.Println("Got", x, "grouping on", k)
			if current.Key == nil {
				logger.Println("Key was None. Setting to", k)
				current.Key = k
				out <- current
			} else if !equals(current.Key, k) {
				logger.Println("Keys unequal", current.Key, "!=", k)
				close(current.Values)
				current = &Group{k, make(chan interface{})}
				out <- current
			}
			current.Values <- x
		}
		close(current.Values)
		close(out)
	}()
	return out
}
