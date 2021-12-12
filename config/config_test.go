package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/davecgh/go-spew/spew"
	lk "github.com/digisan/logkit"
)

func TestConfig(t *testing.T) {
	cfg := &ReportConfig{}
	_, err := toml.DecodeFile("./config.toml", cfg)
	lk.FailOnErr("%v", err)
	fmt.Println("-------------------------------")
	spew.Dump(cfg)
}
