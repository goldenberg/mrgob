package main

import (
	"unicode"
	"flag"
	"io"
	"os"
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
	var runMapper = flag.Bool("mapper", false, "Run the mapper")
	var runReducer = flag.Bool("reducer", false, "Run the mapper")
	flag.Parse()
	j := new(MRWordCount)
	job := NewMRJob(j, j)
	if *runMapper {
		job.runMapper(os.Stdin, os.Stdout)
	} else if *runReducer {
		err := job.runReducer(os.Stdin, os.Stdout)
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
}
