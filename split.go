package splitter

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	csvtool "github.com/cdutwhu/csv-tool"
	gio "github.com/digisan/gotk/io"
	"github.com/gosuri/uiprogress"
	"github.com/nsip/dev-nrt-splitter/config"
)

var (
	uip      *uiprogress.Progress
	bar      *uiprogress.Bar
	procsize uint64
	progbar  = true
)

func EnableProgBar(enable bool) {
	progbar = enable
}

// NrtSplit :
func NrtSplit(configurations ...string) error {
	defer os.RemoveAll("./tempcsv")

	cfg := config.GetConfig(configurations...)
	// fmt.Println(cfg.InFolder)
	inFolderAbs, err := filepath.Abs(cfg.InFolder)
	if err != nil {
		return err
	}

	// -- progress bar 1 -- //
	if progbar {
		uip = uiprogress.New()
		defer uip.Stop()
		uip.Start()
		cnt, _, err := gio.FileDirCount(inFolderAbs, cfg.WalkSubFolders)
		if err != nil {
			return err
		}
		bar = uip.AddBar(cnt)
		bar.AppendCompleted().PrependElapsed()
	}

	err = filepath.Walk(inFolderAbs, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error [%v] at a path [%q], Check your config.toml [InFolder] \n", err, path)
			return err
		}

		var (
			fPath = path
			fDir  = filepath.Dir(path) + "/"
			// fName = info.Name()
			fExt = filepath.Ext(path)
		)

		//
		fPath, err = filepath.Abs(fPath)
		if err != nil {
			return err
		}
		tailPath := fPath[len(inFolderAbs):]

		if info.IsDir() || fExt != ".csv" {
			return nil
		}

		if !cfg.WalkSubFolders {
			fDirAbs, err := filepath.Abs(fDir)
			if err != nil {
				log.Fatalf("Error when walk through abs %s", inFolderAbs)
			}
			if inFolderAbs != fDirAbs {
				return nil
			}
		}

		// Splitting first
		if cfg.Splitting.Enabled {
			// fmt.Printf("Split Processing...: %v\n", fPath)
			outFile := cfg.Splitting.OutFolder + tailPath
			outFolder := outFile[:strings.LastIndex(outFile, "/")]
			csvtool.Split(fPath, outFolder, false, cfg.Splitting.Schema...)
		}

		if cfg.Trim.Enabled {
			// fmt.Printf("Trim Processing...: %v\n", fPath)
			outFile := cfg.Trim.OutFolder + tailPath
			csvtool.Query(fPath, false, cfg.Trim.Columns, '&', nil, outFile, nil)
		}

		// -- progress bar 2 -- //
		if progbar {
			atomic.AddUint64(&procsize, 1)
			bar.Set(int(procsize))
			bar.Incr()
		}

		return nil
	})

	if err != nil {
		log.Fatalf("error walking the path %q: %v\n", inFolderAbs, err)
	}

	return err
}
