package config

import (
	"log"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config :
type ReportConfig struct {
	InFolder          string
	WalkSubFolders    bool
	TrimColAfterSplit bool
	Trim              struct {
		Columns   []string
		Enabled   bool
		OutFolder string
	}
	Split struct {
		Enabled   bool
		OutFolder string
		Schema    []string
	}
}

// GetConfig :
func GetConfig(configs ...string) *ReportConfig {
	for _, config := range configs {
		cfg := &ReportConfig{}
		_, err := toml.DecodeFile(config, cfg)
		if err != nil {
			continue
		}

		// Dir Process
		cfg.InFolder = strings.TrimSuffix(cfg.InFolder, "/") + "/"
		cfg.Trim.OutFolder = strings.TrimSuffix(cfg.Trim.OutFolder, "/") + "/"
		cfg.Split.OutFolder = strings.TrimSuffix(cfg.Split.OutFolder, "/") + "/"

		return cfg
	}
	log.Fatalln("Report Config File is Missing or Error")
	return nil
}
