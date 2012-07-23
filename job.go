package main

import (
	"bufio"
	"io"
	// "strings"
	"encoding/json"
)

type Key interface {
	String() string
}

type Value interface {
	String() string
}

type Pair struct {
	K interface{}
	V interface{}
}

type Mapper interface {
	Map(x interface{}, out chan Pair)
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan Pair)
}

type MRJob struct {
	mapper  Mapper
	reducer Reducer
}

type PairWriter struct {
	bufio.Writer
}

func NewPairWriter(w io.Writer) *PairWriter {
	return &PairWriter(bufio.NewWriter(w))
}

func (w *PairWriter) WritePair(p Pair) (err error) {
	b, err := json.Marshal(p.K)
	if err != nil {
		return err
	}
	w.Write(b)
	err, n = w.WriteString("\t")
	if err != nil {
		return err
	}
	b, err = json.Marshal(p.V)
	if err != nil {
		return err
	}
	err, n = w.Write(b)
	if err != nil {
		return err
	}
	w.WriteString("\n")
}

func (j *MRJob) runMapper(in io.Reader, out io.Writer) (err error) {
	bufIn := bufio.NewReader(in)
	pairOut := NewPairWriter(out)
	mapperOut := make(chan Pair)

	go func() {
		for p := range mapperOut {
			pairOut.WritePair(p)
		}
	}()

	for {
		line, err := bufIn.ReadString('\n')
		if err != nil {
			return err
		}
		j.Map(line, mapperOut)
	}
	close(mapperOut)
}
