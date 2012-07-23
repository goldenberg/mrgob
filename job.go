package main

import (
	"bufio"
	"io"
	// "fmt"
	"strings"
	"encoding/json"
)

type Pair struct {
	K, V interface{}
}

type Mapper interface {
	Map(x interface{}, out chan Pair) error
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan Pair) error
}

type MRJob struct {
	mapper  Mapper
	reducer Reducer
}

func NewMRJob(m Mapper, r Reducer) *MRJob {

	return &MRJob{m, r}	
}

type JSONPairWriter struct {
	w *bufio.Writer
}

func NewPairWriter(w io.Writer) *JSONPairWriter {
	return &JSONPairWriter{bufio.NewWriter(w)}
}

func (w *JSONPairWriter) Write(p Pair) (err error) {
	b, err := json.Marshal(p.K)
	if err != nil {
		return err
	}
	w.w.Write(b)
	_, err = w.w.WriteString("\t")
	if err != nil {
		return err
	}
	b, err = json.Marshal(p.V)
	if err != nil {
		return err
	}
	_, err = w.w.Write(b)
	if err != nil {
		return err
	}
	w.w.WriteString("\n")
	return nil
}

type JSONPairReader struct {
	r *bufio.Reader
}

func NewPairReader(w io.Reader) *JSONPairReader {
	return &JSONPairReader{bufio.NewReader(w)}	
}

func (r *JSONPairReader) Read() (*Pair, error) {
	line, err := r.r.ReadString("\n")	
	if err != nil {
		return err
	}

	fields := strings.SplitN(line, "\t", 1)
	var key interface{}
	var val interface{}
	err := json.Unmarshal(fields[0], key)
	if err != nil {
		return err
	}
	err := json.Unmarshal(fields[1], val)
	if err != nil {
		return err
	}
	return &Pair{key, val}
}

func (j *MRJob) runMapper(in io.Reader, out io.Writer) (err error) {
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

func (j *MRJob) runReducer(in io.Reader, out io.Writer) error {
	bufIn := bufio.NewReader(in)
	pairOut := NewPairWriter(out)
	reducerOut := make(chan Pair)

	go func() {
		for p := range reducerOut {
			err := pairOut.Write(p)
			if err != nil {
				panic(err)
			}
		}
	}()

	var lastKey interface{}
	var valChan chan interface{}

	for {
		line, err := bufIn.ReadString('\n')
		if err != nil {
			return err
		}
		p := NewPair(line)
		if lastKey == p.K {
			valChan <- p.V
		} else {
			lastKey = p.K
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
