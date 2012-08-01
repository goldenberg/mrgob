package main

import (
flag "github.com/ogier/pflag"
	"fmt"
	"io"
	"log"
	"os"
)

var _ = fmt.Sprintln

var logger = log.New(os.Stderr, "", 0)

type Mapper interface {
	Map(x interface{}, out chan interface{}) error
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan interface{}) error
}

type Job struct {
	Map          Mapper
	Reduce       Reducer
	MapReader    Reader
	MapWriter    PairWriter
	ReduceReader PairReader
	ReduceWriter PairWriter
}

func NewJob(mapper Mapper, reducer Reducer) *Job {
	return &Job{
		Map:          mapper,
		Reduce:       reducer,
		MapReader:    NewLineReader(os.Stdin),
		MapWriter:    NewPairWriter(os.Stdout),
		ReduceReader: NewPairReader(os.Stdin),
		ReduceWriter: NewPairWriter(os.Stdout),
	}
}

func (j *Job) runMapper() (err error) {
	mapperOut := make(chan interface{})
	done := make(chan bool)

	// Write the map output
	go func() {
		for p := range mapperOut {
			err := j.MapWriter.Write(p.(*Pair))
			if err != nil {
				return
			}
		}
		j.MapWriter.Flush()
		done <- true
	}()

	for {
		x, err := j.MapReader.Read()
		if err != nil || x == nil {
			close(mapperOut)
			break
		}
		j.Map.Map(x, mapperOut)
	}
	<-done
	return nil
}

func (j *Job) runReducer() error {
	reducerIn := make(chan interface{})
	reducerOut := make(chan interface{})
	done := make(chan bool)

	// Iterate over the groups, executing Reduce() synchronously.
	// Someday this can be changed to have multiple workers.
	go func() {
		for group := range GroupBy(reducerIn, pairKey) {
			j.Reduce.Reduce(group.Key, group.Values, reducerOut)
		}
		close(reducerOut)
	}()

	// Write the output from the tasks
	go func() {
		defer j.ReduceWriter.Flush()

		for p := range reducerOut {
			err := j.ReduceWriter.Write(p.(*Pair))
			if err != nil && err == io.EOF {
				return
			}
		}
		done <- true
	}()

	// Read into the reducers
	for {
		current, err := j.ReduceReader.Read()
		if err != nil {
			close(reducerIn)
			break
		}
		reducerIn <- current
	}
	<-done
	return nil
}

func (r *Job) Run() {
	var runMapper = flag.Bool("mapper", false, "Run the mapper")
	var runReducer = flag.Bool("reducer", false, "Run the mapper")
	flag.Parse()

	if *runMapper {
		r.runMapper()
	} else if *runReducer {
		err := r.runReducer()
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
	return
}
