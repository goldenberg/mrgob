package main

import (
	// "io"
	// "bufio"
)

// type KVReader struct {
// 	io.Reader
// }

// func NewKVReader(r io.Reader) *KVReader {
// 	return &KVReader{bufio.NewReader(r)}
// }

// func (r *KVReader) Next() (Pair, error) {
// 	key, err := r.ReadBytes('\t')
// }

// type KVWriter struct {
// 	io.Writer
// }

// func (w *KVWriter) Next(p Pair) error {
// 	line := []bytes(strings.Join([]string{p.K.String(), p.V.String()}, "\t"))
// 	n, err := w.write(line)
// 	if err != nil {
// 		return err
// 	}
// }
