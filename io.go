package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

type JSONPairWriter struct {
	*bufio.Writer
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
	_, err = w.WriteString(strings.Join([]string{string(key), string(val)}, "\t"))
	w.WriteByte('\n')
	if err != nil {
		return err
	}
	return nil
}

type JSONPairReader struct {
	*bufio.Reader
}

func NewPairReader(r io.Reader) *JSONPairReader {
	return &JSONPairReader{bufio.NewReader(r)}
}

func (r *JSONPairReader) Read() (*Pair, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	fields := strings.SplitN(line, "\t", 2)

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
	return &Pair{key, val}, nil
}
