package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	// "encoding/csv"
)

var _ fmt.Scanner

type Pair struct {
	K, V interface{}
}

type Mapper interface {
	Map(x interface{}, out chan Pair) error
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan Pair) error
}

type Runner struct {
	mapper  Mapper
	reducer Reducer
}

func NewRunner(m Mapper, r Reducer) *Runner {

	return &Runner{m, r}
}

type JSONPairWriter struct {
	w *bufio.Writer 
}

func NewPairWriter(w io.Writer) *JSONPairWriter {
	return &JSONPairWriter{bufio.NewWriter(w)}
}

func (w *JSONPairWriter) Write(p Pair) (err error) {
	key, err := json.Marshal(p.K)
	if err != nil {
		return err
	}
	val, err := json.Marshal(p.V)
	if err != nil {
		return err
	}
	_, err = w.w.WriteString(strings.Join([]string{string(key), string(val)}, "\t"))
	w.w.WriteByte('\n')
	if err != nil {
		return err
	}
	return nil
}

func (w *JSONPairWriter) Flush() {
	w.w.Flush()
}



type JSONPairReader struct {
	r *bufio.Reader
}

func NewPairReader(r io.Reader) *JSONPairReader {
	return &JSONPairReader{bufio.NewReader(r)}
}

func (r *JSONPairReader) Read() (*Pair, error) {
	line, err := r.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	fields := strings.SplitN(line, "\t", 2)

	// fmt.Println("fields:", fields)
	var key interface{}
	var val interface{}
	err = json.Unmarshal([]byte(strings.TrimSpace(fields[0])), &key)
	if err != nil {
		fmt.Println("bad key", key, "on line", line)
		return nil, err
	}
	err = json.Unmarshal([]byte(strings.TrimSpace(fields[1])), &val)
	if err != nil {
		fmt.Println("bad val", val, "on line", line)
		return nil, err
	}
	// fmt.Println("key:", key)
	return &Pair{key, val}, nil
}

func (j *Runner) runMapper(in io.Reader, out io.Writer) (err error) {
	bufIn := bufio.NewReader(in)
	pairOut := NewPairWriter(out)
	mapperOut := make(chan Pair)

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


type ReduceTask struct {
	key interface{}
	vals chan interface{}	
	reducer Reducer
}

func (j *Runner) runReducer(in io.Reader, out io.Writer) error {
	pairIn := NewPairReader(in)
	reducerOut := make(chan Pair)

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

	// Keep track of the current key.
	var curKey *string
	vals := make([]interface{}, 0)

	for {
		p, err := pairIn.Read()
		if err != nil {
			return err
		}
		if curKey == nil {
			k := p.K.(string)
			curKey = &k
		}
		// If we're on the same key, send it on
		if *curKey == p.K.(string) {
			vals = append(vals, p.V)

		// If the key switched start reducing.
		} else {
			// Run the reducer synchronously
			valChan := make(chan interface{})
			go func() {
				j.reducer.Reduce(*curKey, valChan, reducerOut)
			}()
			for _, v := range vals {
				valChan <- v		
			}
			close(valChan)

			k := p.K.(string)
			curKey = &k
			vals = make([]interface{}, 0)
			vals = append(vals, p.V)
		}
	}
	close(reducerOut)
	return nil
}

func (r *Runner) Run() {
	
}