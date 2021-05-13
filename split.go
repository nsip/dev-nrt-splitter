package splitter

import (
	"bytes"
	"io"
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

	mFileEmptyCSV := make(map[string][]byte)

	err = filepath.Walk(inFolderAbs, func(path string, info os.FileInfo, err error) error {

		if err != nil {
			log.Fatalf("Error [%v] at a path [%q], Check your config.toml [InFolder] \n", err, path)
			return err
		}

		if info.IsDir() || filepath.Ext(path) != ".csv" {
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

		tailPath := path[len(inFolderAbs):]

		// Split first
		if cfg.Split.Enabled {
			// fmt.Printf("Split Processing...: %v\n", path)

			// csvtool.ForceSingleProc(true)
			csvtool.KeepCatHeaders(false)
			csvtool.KeepIgnCatHeaders(true)
			csvtool.StrictSchema(true)
			csvtool.Dir4NotSplittable(cfg.Split.IgnoreFolder)

			outFile := filepath.Join(cfg.Split.OutFolder, tailPath)
			outFolder := outFile[:strings.LastIndex(outFile, "/")]
			splitfiles, ignoredfiles, _ := csvtool.Split(path, outFolder, cfg.Split.Schema...)

			// trim columns also apply to split result if set
			if cfg.Trim.Enabled && cfg.TrimColAfterSplit {
				for _, sf := range splitfiles {
					csvtool.QueryFile(sf, false, cfg.Trim.Columns, '&', nil, sf)
				}
			}

			// find valid schema, empty content file to spread
			for _, ignf := range ignoredfiles {

				hdrs, n, _ := csvtool.FileInfo(ignf)
				if err != nil {
					log.Printf("%v @ %s", err, ignf)
					return err
				}

				if n == 0 && ts.Superset(hdrs, cfg.Split.Schema) {
					emptycsv, err := os.ReadFile(ignf)
					if err != nil {
						log.Printf("%v @ %s", err, ignf)
						return err
					}

					// Trim Columns if needed
					rmHdrs := cfg.Split.Schema
					if cfg.Trim.Enabled && cfg.TrimColAfterSplit {
						rmHdrs = ts.MkSet(append(rmHdrs, cfg.Trim.Columns...)...)
					}
					var buf bytes.Buffer
					csvtool.Subset(emptycsv, false, rmHdrs, false, nil, io.Writer(&buf))
					emptycsv = buf.Bytes()

					mFileEmptyCSV[ignf] = emptycsv
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

	// spread all valid schema, empty csv to each split folder
	mOutRecord := make(map[string]struct{})
	if cfg.Split.Enabled {
		err = filepath.Walk(cfg.Split.OutFolder, func(path string, info os.FileInfo, err error) error {
			if info.IsDir() || filepath.Ext(path) != ".csv" {
				return nil
			}
			mOutRecord[filepath.Dir(path)] = struct{}{}
			return nil
		})
		if err != nil {
			log.Fatalf("error walking the path %q: %v\n", inFolderAbs, err)
		}

		nSchema := len(cfg.Split.Schema)
		for outpath := range mOutRecord {
			for emptypath, csv := range mFileEmptyCSV {
				emptyroot := filepath.Base(filepath.Dir(emptypath))
				emptyfile := filepath.Base(emptypath)
				outdir := outpath
				for i := 0; i < nSchema; i++ {
					outdir = filepath.Dir(outdir)
				}
				if strings.HasSuffix(outdir, "/"+emptyroot) {
					gotkio.MustWriteFile(filepath.Join(outpath, emptyfile), csv)
				}
			}
		}
	}

	return err
}
