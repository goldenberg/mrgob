package main

import (
"io"
"bufio"
"encoding/json"
"strings"
"fmt"
)

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