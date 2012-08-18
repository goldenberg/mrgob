package main

import (
	"encoding/json"
	"fmt"
	flag "github.com/ogier/pflag"
	"io"
	"log"
	"os"
)


 var _ = fmt.Sprintln
var logger = log.New(os.Stderr, "", 0)

type Mapper interface {
	Map(x interface{}, out chan interface{}) error
}

type Reducer interface {
	Reduce(key interface{}, values chan interface{}, out chan interface{}) error
}

type Step struct {
	Map          Mapper
	Reduce       Reducer
	MapReader    Reader
	MapWriter    PairWriter
	ReduceReader PairReader
	ReduceWriter PairWriter
}

type Job struct {
	Steps []Step
}

func NewJob(steps ...Step) *Job {
	return &Job{steps}
}

func (j *Job) printSteps() (err error) {
	descriptions := make([]map[string]interface{}, 0)
	for _, step := range j.Steps {
		descriptions = append(descriptions, step.describe())
	}

	b, err := json.Marshal(descriptions)
	if err != nil {
	       return err
	}
	os.Stdout.Write(b)
	os.Stdout.WriteString("\n")
	return nil
}

func NewStep(mapper Mapper, reducer Reducer) *Step {
	return &Step{
		Map:          mapper,
		Reduce:       reducer,
		MapReader:    NewLineReader(os.Stdin),
		MapWriter:    NewPairWriter(os.Stdout),
		ReduceReader: NewPairReader(os.Stdin),
		ReduceWriter: NewPairWriter(os.Stdout),
	}
}

func (j *Step) runMapper() (err error) {
	mapperOut := make(chan interface{})
	done := make(chan bool)

	// Write the map output
	go func() {
		for p := range mapperOut {
			err := j.MapWriter.Write(p.(*Pair))
			if err != nil {
				return
			}
		}
		j.MapWriter.Flush()
		done <- true
	}()

	for {
		x, err := j.MapReader.Read()
		if err != nil || x == nil {
			close(mapperOut)
			break
		}
		j.Map.Map(x, mapperOut)
	}
	<-done
	return nil
}

func (j *Step) runReducer() error {
	reducerIn := make(chan interface{})
	reducerOut := make(chan interface{})
	done := make(chan bool)

	// Iterate over the groups, executing Reduce() synchronously.
	// Someday this can be changed to have multiple workers.
	go func() {
		for group := range GroupBy(reducerIn, pairKey) {
			j.Reduce.Reduce(group.Key, group.Values, reducerOut)
		}
		close(reducerOut)
	}()

	// Write the output from the tasks
	go func() {
		defer j.ReduceWriter.Flush()

		for p := range reducerOut {
			err := j.ReduceWriter.Write(p.(*Pair))
			if err != nil && err == io.EOF {
				return
			}
		}
		done <- true
	}()

	// Read into the reducers
	for {
		current, err := j.ReduceReader.Read()
		if err != nil {
			close(reducerIn)
			break
		}
		reducerIn <- current
	}
	<-done
	return nil
}

func (j *Step) describe() map[string]interface{} {
	out := make(map[string]interface{}, 0)

	out["type"] = "streaming"

	if j.Map != nil {
		out["mapper"] = map[string]string{
			"type":       "script",
			"pre_filter": "cat",
		}
	}
	if j.Reduce != nil {
		out["reducer"] = map[string]string{
			"type":       "script",
			"pre_filter": "cat",
		}
	}
	return out
}

func (j *Job) Run() {
	var runMapper = flag.Bool("mapper", false, "Run the mapper")
	var runReducer = flag.Bool("reducer", false, "Run the mapper")
	var printSteps = flag.Bool("steps", false, "Print step descriptions")

	var stepNum = flag.Uint("step-num", 0, "Step number")

	flag.Parse()

	if *runMapper {
		j.Steps[*stepNum].runMapper()
	} else if *runReducer {
		err := j.Steps[*stepNum].runReducer()
		if err != nil && err != io.EOF {
			panic(err)
		}
	} else if *printSteps {
		j.printSteps()
	}
	return
}
