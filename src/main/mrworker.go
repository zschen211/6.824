package main

/*
	start a worker process, which is implemented
	in ../mr/worker.go. typically there will be
	multiple worker processes, talking to one coordinator.

	go run mrworker.go wc.so

	Please do not change this file.
*/

import (
	. "6.824/logging"
	"6.824/mr"
	"fmt"
	"os"
	"plugin"
)

func main() {
	if len(os.Args) != 2 {
		_, err := fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		if err != nil {
			return
		}
		os.Exit(1)
	}

	mapFunc, reduceFunc := loadPlugin(os.Args[1])
	Logger.Info(fmt.Sprintf("Loaded map & reduce function from plugin: %s", os.Args[1]))

	mr.Worker(mapFunc, reduceFunc)
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		Logger.Fatal(fmt.Sprintf("cannot load plugin %v. error message: %s", filename, err.Error()))
	}
	xmapFunc, err := p.Lookup("Map")
	if err != nil {
		Logger.Fatal(fmt.Sprintf("cannot find Map in %v", filename))
	}
	mapFunc := xmapFunc.(func(string, string) []mr.KeyValue)
	xreduceFunc, err := p.Lookup("Reduce")
	if err != nil {
		Logger.Fatal(fmt.Sprintf("cannot find Reduce in %v", filename))
	}
	reduceFunc := xreduceFunc.(func(string, []string) string)

	return mapFunc, reduceFunc
}
