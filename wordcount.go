package main

import (
	"unicode"
	"strings"
)

const ()

type MRWordCount struct{}

func isPunctOrSpace(r rune) bool {
	return unicode.IsPunct(r) || unicode.IsSpace(r)
}

func (j *MRWordCount) Map(line interface{}, out chan Pair) error {
	for _, word := range strings.FieldsFunc(line.(string), isPunctOrSpace) {
		if len(word) > 0 {
			out <- Pair{strings.ToLower(word), 1}
		}
	}
	return nil
}

func (j *MRWordCount) Reduce(key interface{}, values chan interface{}, out chan Pair) error {
	sum := 0.
	for val := range values {
		sum += val.(float64)
	}
	out <- Pair{key.(string), sum}
	return nil
}

func main() {
	job := new(MRWordCount)
	runner := NewRunner(job, job)
	runner.Run()
}
