package main

import (
	"log"
	"os"

	splitter "github.com/nsip/dev-nrt-splitter"
)

func main() {

	configurations := []string{}
	if len(os.Args) > 1 {
		configurations = append(configurations, os.Args[1:]...)
	}
	configurations = append(configurations, "./config.toml")
	if err := splitter.NrtSplit(configurations...); err != nil {
		log.Fatalln(err)
	}

}
