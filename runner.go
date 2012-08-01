package main

import (
	"flag"
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
	reducerOut := make(chan interface{})
	reducerIn := make(chan interface{})
	done := make(chan bool)

	// Write the output
	go func() {
		logger.Println("starting reduce output goroutine")
		defer j.ReduceWriter.Flush()

		for p := range reducerOut {
			err := j.ReduceWriter.Write(p.(*Pair))
			if err != nil && err == io.EOF {
				return
			}
		}
		done <- true
	}()

	go func() {
		logger.Println("starting reduce goroutine")
		for group := range GroupBy(reducerIn, pairKey) {
			logger.Println("Got group", group)
			j.Reduce.Reduce(group.Key, group.Values, reducerOut)
		}
		close(reducerOut)
	}()

	// Read into the reducers
	for {
		current, err := j.ReduceReader.Read()
		logger.Println("Read", current)
		if err != nil {
			close(reducerIn)
			break
		}
		reducerIn <- current
	}
	logger.Println("Waiting for done")
	<-done
	return nil
	// // Keep track of the current key, assumed to be a string...
	// var last *Pair
	// vals := make([]interface{}, 0)

	// for {
	// 	current, err := j.ReduceReader.Read()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if last == nil || last.Equals(current) {
	// 		fmt.Println("last", last, "current", current)
	// 		vals = append(vals, current.Value)

	// 		// If the key switched start reducing.
	// 	} else {
	// 		// Run the reducer synchronously
	// 		valChan := make(chan interface{})
	// 		go func() {
	// 			j.Reduce.Reduce(last.Key, valChan, reducerOut)
	// 		}()
	// 		for _, v := range vals {
	// 			valChan <- v
	// 		}
	// 		close(valChan)

	// 		vals = make([]interface{}, 0)
	// 		vals = append(vals, current.Value)
	// 	}
	// 	last = current
	// }
	close(reducerOut)
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
