package main

import (
	"strings"
	"unicode"
)

type MRWordCount struct{}

func isPunctOrSpace(r rune) bool {
	return unicode.IsPunct(r) || unicode.IsSpace(r)
}

func (j *MRWordCount) Map(line interface{}, out chan interface{}) error {
	for _, word := range strings.FieldsFunc(line.(string), isPunctOrSpace) {
		if len(word) > 0 {
			out <- &Pair{strings.ToLower(word), 1}
		}
	}
	return nil
}

func (j *MRWordCount) Reduce(key interface{}, values chan interface{}, out chan interface{}) error {
	sum := 0.
	for val := range values {
		sum += val.(*Pair).Value.(float64)
	}
	out <- &Pair{key.(string), sum}
	return nil
}

func main() {
	wc := new(MRWordCount)
	job := NewJob(*NewStep(wc, wc))
	job.Run()
}
