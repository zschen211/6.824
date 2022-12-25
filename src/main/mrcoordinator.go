package main

//
// start the coordinator process, which is implemented
// in ../mr/coordinator.go
//
// go run mrcoordinator.go pg*.txt
//
// Please do not change this file.
//

import (
	. "6.824/logging"
	"6.824/mr"
	"fmt"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		_, err := fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
		if err != nil {
			return
		}
		os.Exit(1)
	}

	Logger.Info("Initializing Coordinator......")
	m := mr.MakeCoordinator(os.Args[1:], 10)

	Logger.Info("Initialization finished !")
	<-m.Finished

	time.Sleep(time.Second)
}
