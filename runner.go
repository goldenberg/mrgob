package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

var _ = fmt.Sprintln

type Pair struct {
	Key, Value interface{}
}

type Comparator interface {
	Equals(x interface{}) bool
}

func equals(x, y interface{}) bool {
	switch x.(type) {
	case Comparator:
		return x.(Comparator).Equals(y)
	}
	return false
}

func (p *Pair) Equals(x interface{}) bool {
	switch x.(type) {
	case *Pair:
		return equals(p.Key, x.(*Pair).Key)
	case Pair:
		return equals(p.Key, x.(Pair).Key)
	}
	return false
}

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
	defer close(mapperOut)

	go func() {
		for p := range mapperOut {
			err := j.MapWriter.Write(p.(*Pair))
			if err != nil {
				return
			}
		}
		j.MapWriter.Flush()
	}()

	for {
		x, err := j.MapReader.Read()
		if err != nil {
			return err
		}
		j.Map.Map(x, mapperOut)
	}
	return nil
}

func (j *Job) runReducer() error {
	reducerOut := make(chan interface{})

	// Write the output
	go func() {
		defer j.ReduceWriter.Flush()

		for p := range reducerOut {
			err := j.ReduceWriter.Write(p.(*Pair))
			if err != nil && err == io.EOF {
				return
			}
		}
	}()

	// Keep track of the current key, assumed to be a string...
	var last *Pair
	vals := make([]interface{}, 0)

	for {
		current, err := j.ReduceReader.Read()
		if err != nil {
			return err
		}
		if last == nil || last.Equals(current) {
			fmt.Println("last", last, "current", current)
			vals = append(vals, current.Value)

			// If the key switched start reducing.
		} else {
			// Run the reducer synchronously
			valChan := make(chan interface{})
			go func() {
				j.Reduce.Reduce(last.Key, valChan, reducerOut)
			}()
			for _, v := range vals {
				valChan <- v
			}
			close(valChan)

			vals = make([]interface{}, 0)
			vals = append(vals, current.Value)
		}
		last = current
	}
	close(reducerOut)
	return nil
}

func groupBy(c chan interface{}, key func(x, y interface{}) bool) (out chan chan interface{}) {
	out = make(chan chan interface{})
	var last interface{}

	for x := range c {
		if last == nil {
			last = x
			continue
		}
	}	
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
}
