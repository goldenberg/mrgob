package main

import (
	"flag"
	"io"
	"os"
	"strings"
)

const ()

type MRWordCount struct{}

func (j *MRWordCount) Map(line interface{}, out chan Pair) error {
	STOP_TOKENS := []string{
		`'`, `"`, `,`,
		`\n`, `\t`,
		`;`, `,`, `.`,
		`(`, `)`, `:`,
		`]`, `[`}
	for _, word := range strings.Fields(line.(string)) {
		word = strings.TrimSpace(strings.ToLower(word))
		for _, t := range STOP_TOKENS {
			word = strings.Replace(word, t, "", -1)
		}
		if len(word) > 0 {
			out <- Pair{word, 1}
		}
	}
	return nil
}

func (j *MRWordCount) Reduce(key interface{}, values chan interface{}, out chan Pair) error {
	sum := 0.
	for val := range values {
		// i, err := strconv.Atoi(val.(string))
		// if err != nil {
		// 	return err
		// }
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
