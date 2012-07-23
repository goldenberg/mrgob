package main

import (
// "strconv"
	"strings"
	"os"
	"flag"
)

type MRWordCount struct {}

func (j *MRWordCount) Map(line interface{}, out chan Pair) error {
	for _, word := range strings.Split(line.(string), " ") {
		out <- Pair{strings.TrimSpace(strings.ToLower(word)), 1}
	}
	return nil
}

func (j *MRWordCount) Reduce(key interface{}, values chan interface{}, out chan Pair) error {
	sum := 0
	for val := range values {
		// i, err := strconv.Atoi(val.(string))
		// if err != nil {
		// 	return err
		// }
		sum += val.(int)
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
	} else if *runReducer{
		job.runReducer(os.Stdin, os.Stdout)
	}
}

