package splitter

import (
	cfg "github.com/digisan/go-config"
	"github.com/gosuri/uiprogress"
)

var (
	uip      *uiprogress.Progress
	bar      *uiprogress.Bar
	procSize uint64
	progBar  = true

	enableTrim         bool
	enableSplit        bool
	inFolder           string
	inFolderAbs        string
	goSubFolder        bool
	bySplit2           bool
	ignoreFolder4Split string
	out4Split          string
	splitSchema        []string
	trimColAfterSplit  bool
	trimCols           []string
	out4Trim           string
	merges             []map[string]any
)

func init() {
	// setConfig("./config.toml", "../config.toml")
}

func setConfig(fConfigs ...string) {

	cfg.Init("config", false, fConfigs...)

	inFolder = cfg.Path("InFolder")
	inFolderAbs = cfg.PathAbs("InFolder")
	goSubFolder = cfg.Bool("WalkSubFolders")
	trimColAfterSplit = cfg.Bool("TrimColAfterSplit")

	enableTrim = cfg.Bool("Trim.Enabled")
	trimCols = cfg.Strs("Trim.Columns")
	out4Trim = cfg.Path("Trim.OutFolder")

	enableSplit = cfg.Bool("Split.Enabled")
	bySplit2 = cfg.Bool("Split.Split2")
	ignoreFolder4Split = cfg.Path("Split.IgnoreFolder")
	out4Split = cfg.Path("Split.OutFolder")
	splitSchema = cfg.Strs("Split.Schema")

	merges = cfg.Objects("Merge")
}
