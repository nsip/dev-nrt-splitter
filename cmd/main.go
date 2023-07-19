package main

import (
	"os"

	lk "github.com/digisan/logkit"
	splitter "github.com/nsip/dev-nrt-splitter"
)

func main() {
	config := []string{}
	if len(os.Args) > 1 {
		config = append(config, os.Args[1:]...)
	}
	config = append(config, "./config.toml")
	lk.WarnOnErr("%v", splitter.NrtSplit(config...))
}
