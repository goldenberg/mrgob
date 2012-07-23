package main

import (
	"strings"
)

type MRWordCount struct{}

func (j *MRWordCount) Map(line string, out chan Pair) {
	for _, word := range strings.Split(line, " ") {
		out <- Pair{word, 1}
	}
}

func (j *MRWordCount) Reduce(key interface{}, values chan interface{}, out chan Pair) {
	sum := 0
	for val := range values {
		sum += val.(int)
	}
	out <- Pair{key, sum}
}
