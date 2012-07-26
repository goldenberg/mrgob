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
	w *bufio.Writer //*csv.Writer
}

func NewPairWriter(w io.Writer) *JSONPairWriter {
	// csvW := csv.NewWriter(w)
	// csvW.Comma = '\t'
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
		return nil, err
	}
	err = json.Unmarshal([]byte(strings.TrimSpace(fields[1])), &val)
	if err != nil {
		panic(err)
		return nil, err
	}
	// fmt.Println("key:", key)
	return &Pair{key, val}, nil
}

func (j *Runner) runMapper(in io.Reader, out io.Writer) (err error) {
	bufIn := bufio.NewReader(in)
	pairOut := NewPairWriter(out)
	mapperOut := make(chan Pair)

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
	close(mapperOut)
	return nil
}

func (j *Runner) runReducer(in io.Reader, out io.Writer) error {
	pairIn := NewPairReader(in)
	pairOut := NewPairWriter(out)
	reducerOut := make(chan Pair)

	go func() {
		for p := range reducerOut {
			err := pairOut.Write(p)
			if err != nil && err == io.EOF {
				return
			}
		}
	}()

	var lastKey string
	var valChan chan interface{}

	for {
		p, err := pairIn.Read()
		if err != nil {
			if err == io.EOF {
				return err
			} else {
				fmt.Errorf("Bad line: %v", err)
				continue
			}
		}
		thisKey, _ := json.Marshal(p.K)
		if lastKey == string(thisKey) {
			valChan <- p.V
		} else {
			lastKeyBytes, _ := json.Marshal(p.K)
			lastKey = string(lastKeyBytes)
			if valChan != nil {
				close(valChan)
			}
			valChan = make(chan interface{})
			go func() {
				j.reducer.Reduce(lastKey, valChan, reducerOut)
			}()
		}
	}
	close(valChan)
	close(reducerOut)
	return nil
}
