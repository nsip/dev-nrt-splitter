package config

import (
	"log"
	"path/filepath"

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
		Enabled      bool
		OutFolder    string
		Schema       []string
		IgnoreFolder string
		Split2       bool
	}
	Merge []struct {
		Enabled    bool
		MergedName string
		Schema     []string
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
		cfg.InFolder = filepath.Clean(cfg.InFolder)
		cfg.Trim.OutFolder = filepath.Clean(cfg.Trim.OutFolder)
		cfg.Split.OutFolder = filepath.Clean(cfg.Split.OutFolder)
		cfg.Split.IgnoreFolder = filepath.Clean(cfg.Split.IgnoreFolder)

		return cfg
	}
	log.Fatalln("Report Config File is Missing or Error")
	return nil
}
