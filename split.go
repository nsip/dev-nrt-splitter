package splitter

import (
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	csvtool "github.com/cdutwhu/csv-tool"
	gotkio "github.com/digisan/gotk/io"
	"github.com/digisan/gotk/slice/ts"
	"github.com/gosuri/uiprogress"
	"github.com/gosuri/uiprogress/util/strutil"
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

	cfg := config.GetConfig(configurations...)
	// fmt.Println(cfg.InFolder)
	inFolderAbs, err := filepath.Abs(cfg.InFolder)
	if err != nil {
		return err
	}

	// if Split-Schema is identical to Trim-Columns, Do NOT need to trim in each split outcome file
	ignoreTrimInSplit := ts.Equal(cfg.Split.Schema, cfg.Trim.Columns)

	// -- progress bar 1 -- //
	if progbar {
		uip = uiprogress.New()
		defer uip.Stop()
		uip.Start()
		cnt, _, err := gotkio.FileDirCount(inFolderAbs, cfg.WalkSubFolders)
		if err != nil {
			return err
		}
		bar = uip.AddBar(cnt)
		bar.AppendCompleted().PrependElapsed()
		bar.PrependFunc(func(b *uiprogress.Bar) string {
			return strutil.Resize(" Trimming & Splitting...:", 35)
		})
	}

	err = filepath.Walk(inFolderAbs, func(path string, info os.FileInfo, err error) error {

		if err != nil {
			log.Fatalf("Error [%v] at a path [%q], Check your config.toml [InFolder] \n", err, path)
			return err
		}

		// dir, err := filepath.Abs(filepath.Dir(os.Args[0]))
		// dir, err := filepath.Abs(os.Args[0])
		// if err != nil {
		// 	log.Fatal(err)
		// }
		// fmt.Println(dir)
		// fmt.Println(path)

		var (
			fExt     = filepath.Ext(path)
			tailPath = path[len(inFolderAbs):]
		)

		if info.IsDir() || fExt != ".csv" {
			return nil
		}

		if !cfg.WalkSubFolders {
			fDirAbs, err := filepath.Abs(filepath.Dir(path))
			if err != nil {
				log.Fatalf("Error when walk through abs %s", inFolderAbs)
			}
			if inFolderAbs != fDirAbs {
				return nil
			}
		}

		// Split first
		if cfg.Split.Enabled {
			// fmt.Printf("Split Processing...: %v\n", path)

			csvtool.StrictSchema(cfg.Split.StrictMode)
			csvtool.Dir4NotSplittable(cfg.Split.IgnoreFolder)

			outFile := filepath.Join(cfg.Split.OutFolder, tailPath)
			outFolder := outFile[:strings.LastIndex(outFile, "/")]
			splitfiles, _ := csvtool.Split(path, outFolder, false, cfg.Split.Schema...)

			// trim columns also apply to split result if set
			if cfg.TrimColAfterSplit && cfg.Trim.Enabled && !ignoreTrimInSplit {
				for _, sf := range splitfiles {
					csvtool.QueryFile(sf, false, cfg.Trim.Columns, '&', nil, sf)
				}
			}
		}

		if cfg.Trim.Enabled {
			// fmt.Printf("Trim Processing...: %v\n", path)

			outFile := filepath.Join(cfg.Trim.OutFolder, tailPath)
			csvtool.QueryFile(path, false, cfg.Trim.Columns, '&', nil, outFile)
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
