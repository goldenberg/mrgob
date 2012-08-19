package main

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

// A Writer writes one item at a time to an output stream.
type Writer interface {
	// Write a single item.
	Write(x interface{}) error
	// Flush any buffered output. Called before exiting.
	Flush() error
}

// A Reader reads one item at a time from an input source.
type Reader interface {
	Read() (interface{}, error)
}

// LineReader reads newline delimited strings.
type LineReader struct {
	*bufio.Reader
}

// Read an entire line, one at time. Not buffered, so output could potentially
// be very large.
func (r *LineReader) Read() (interface{}, error) {
	return r.ReadString('\n')
}

// NewLineReader creates a LineReader from a Reader.
func NewLineReader(r io.Reader) *LineReader {
	return &LineReader{bufio.NewReader(r)}
}

// A PairWriter writes a key-value pair one at a time.
type PairWriter interface {
	Write(p *Pair) error
	Flush() error
}

// A PairReader reads a key-value pair one at a time.
type PairReader interface {
	Read() (*Pair, error)
}

// A JSONPairWriter marshals the key and value as JSON and writes them as a
// tab-delimited string, followed by a new line.
type JSONPairWriter struct {
	*bufio.Writer
}

// NewPairWriter creates a JSONPairWriter that writes to w.
func NewPairWriter(w io.Writer) *JSONPairWriter {
	return &JSONPairWriter{bufio.NewWriter(w)}
}

// Write a single Pair as tab-delimited JSON followed by a new line.
func (w *JSONPairWriter) Write(p *Pair) (err error) {
	key, err := json.Marshal(p.Key)
	if err != nil {
		return err
	}
	val, err := json.Marshal(p.Value)
	if err != nil {
		return err
	}
	// TODO: this is kinda ghetto, should just be writing bytes, not creating strings.
	_, err = w.WriteString(strings.Join([]string{string(key), string(val)}, "\t"))
	if err != nil {
		return err
	}
	w.WriteByte('\n')
	return nil
}

// JSONPairReader reads lines of tab-delimited JSON key-value Pairs.
type JSONPairReader struct {
	*bufio.Reader
}

// NewPairReader creates a JSONPairReader from r.
func NewPairReader(r io.Reader) *JSONPairReader {
	return &JSONPairReader{bufio.NewReader(r)}
}

// Read a single Pair from a tab-delimited JSON line.
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
		return nil, err
	}
	err = json.Unmarshal([]byte(strings.TrimSpace(fields[1])), &val)
	if err != nil {
		return nil, err
	}
	return &Pair{key, val}, nil
}
