package config

import (
	"log"
	"strings"

	"github.com/BurntSushi/toml"
)

// Config :
type ReportConfig struct {
	InFolder       string
	WalkSubFolders bool
	Trim           struct {
		Columns   []string
		Enabled   bool
		OutFolder string
	}
	Splitting struct {
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
		cfg.Splitting.OutFolder = strings.TrimSuffix(cfg.Splitting.OutFolder, "/") + "/"

		return cfg
	}
	log.Fatalln("Report Config File is Missing or Error")
	return nil
}
