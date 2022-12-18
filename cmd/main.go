package main

import (
	"os"

	lk "github.com/digisan/logkit"
	splitter "github.com/nsip/dev-nrt-splitter"
)

func main() {
	configurations := []string{}
	if len(os.Args) > 1 {
		configurations = append(configurations, os.Args[1:]...)
	}
	configurations = append(configurations, "./config.toml")
	lk.WarnOnErr("%v", splitter.NrtSplit(configurations...))
}
