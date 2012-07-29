package main

import (
	"bufio"
	"flag"
	"io"
	"os"
	"fmt"
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
	case ItemComparator:
		return x.(ItemComparator).Equals(y)
	}
	return false
}

func (p *Pair) Equals(x interface{}) bool {
	switch x.(type) {
	case *Pair:
		return equals(p.Key, x.(*Pair).Key)
	}
	return false
}

type Mapper interface {
	Map(x interface{}, out chan interface{}) error
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan interface{}) error
}

type Runner struct {
	mapper  Mapper
	reducer Reducer
}

func NewRunner(m Mapper, r Reducer) *Runner {
	return &Runner{m, r}
}

func (j *Runner) runMapper(in io.Reader, out io.Writer) (err error) {
	bufIn := bufio.NewReader(in)
	pairOut := NewPairWriter(out)
	mapperOut := make(chan interface{})

	defer pairOut.Flush()
	defer close(mapperOut)

	go func() {
		for p := range mapperOut {
			err := pairOut.Write(p)
			if err != nil {
				return
			}
		}
	}()

	for {
		line, err := bufIn.ReadString('\n')
		if err != nil {
			return err
		}
		j.mapper.Map(line, mapperOut)
	}
	return nil
}


func (j *Runner) runReducer(in io.Reader, out io.Writer) error {
	pairIn := NewPairReader(in)
	reducerOut := make(chan interface{})

	// Write the output
	go func() {
		pairOut := NewPairWriter(out)
		defer pairOut.Flush()

		for p := range reducerOut {
			err := pairOut.Write(p)
			if err != nil && err == io.EOF {
				return
			}
		}
	}()

	// Keep track of the current key, assumed to be a string...
	var last *Pair
	vals := make([]interface{}, 0)

	for {
		x, err := pairIn.Read()
		if err != nil {
			return err
		}
		current := x.(*Pair)
		if last == nil || last.Equals(current) {
			vals = append(vals, current.Value)

		// If the key switched start reducing.
		} else {
			// Run the reducer synchronously
			valChan := make(chan interface{})
			go func() {
				j.reducer.Reduce(last.Key, valChan, reducerOut)
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

func (r *Runner) Run() {
	var runMapper = flag.Bool("mapper", false, "Run the mapper")
	var runReducer = flag.Bool("reducer", false, "Run the mapper")
	flag.Parse()

	if *runMapper {
		r.runMapper(os.Stdin, os.Stdout)
	} else if *runReducer {
		err := r.runReducer(os.Stdin, os.Stdout)
		if err != nil && err != io.EOF {
			panic(err)
		}
	}
}
