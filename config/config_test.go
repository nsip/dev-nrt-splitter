package config

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/cdutwhu/debog/fn"
	"github.com/davecgh/go-spew/spew"
)

func TestConfig(t *testing.T) {
	cfg := &ReportConfig{}
	_, err := toml.DecodeFile("./config.toml", cfg)
	fn.FailOnErr("%v", err)
	fmt.Println("-------------------------------")
	spew.Dump(cfg)
}
