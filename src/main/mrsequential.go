package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import (
	"fmt"
	"plugin"
)
import "6.824/mr"
import "os"
import "log"
import "io/ioutil"
import "sort"

const (
	outputFilename = "mr-out-0"
	mapSymbol      = "Map"
	reduceSymbol   = "Reduce"
)

// ByKey for sorting by key.
type ByKey []mr.KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		_, err := fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		if err != nil {
			return
		}
		os.Exit(1)
	}

	mapFunc, reduceFunc := loadPluginSequential(os.Args[1])

	/*
		read each input file,
		pass it to Map,
		accumulate the intermediate Map output.
	*/
	intermediate := []mr.KeyValue{}
	for _, filename := range os.Args[2:] {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapFunc(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	/*
		a big difference from real MapReduce is that all the
		intermediate data is in one place, intermediate[],
		rather than being partitioned into NxM buckets.
	*/

	sort.Sort(ByKey(intermediate))

	outputFile, _ := os.Create(outputFilename)

	/*
		call Reduce on each distinct key in intermediate[],
		and print the result to mr-out-0.
	*/
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reduceFunc(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(outputFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			return
		}

		i = j
	}

	err := outputFile.Close()
	if err != nil {
		return
	}
}

// LoadPlugin
// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPluginSequential(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot load plugin %v. error message: %s", filename, err.Error()))
	}
	xmapFunc, err := p.Lookup("Map")
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot find Map in %v", filename))
	}
	mapFunc := xmapFunc.(func(string, string) []mr.KeyValue)
	xreduceFunc, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatal(fmt.Sprintf("cannot find Reduce in %v", filename))
	}
	reduceFunc := xreduceFunc.(func(string, []string) string)

	return mapFunc, reduceFunc
}
