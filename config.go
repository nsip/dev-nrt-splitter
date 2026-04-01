package splitter

import (
	"path/filepath"

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

	inFolder = cfg.Val[string]("InFolder")
	inFolderAbs, _ = filepath.Abs(cfg.Val[string]("InFolder"))
	goSubFolder = cfg.Val[bool]("WalkSubFolders")
	trimColAfterSplit = cfg.Val[bool]("TrimColAfterSplit")

	enableTrim = cfg.Val[bool]("Trim.Enabled")
	trimCols = cfg.ValArr[string]("Trim.Columns")
	out4Trim = cfg.Val[string]("Trim.OutFolder")

	enableSplit = cfg.Val[bool]("Split.Enabled")
	bySplit2 = cfg.Val[bool]("Split.SplitVer2")
	ignoreFolder4Split = cfg.Val[string]("Split.IgnoreFolder")
	out4Split = cfg.Val[string]("Split.OutFolder")
	splitSchema = cfg.ValArr[string]("Split.Schema")

	merges = cfg.Objects("Merge")
}
