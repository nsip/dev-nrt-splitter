package splitter

import (
	"bytes"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	ct "github.com/cdutwhu/csv-tool"
	fd "github.com/digisan/gotk/filedir"
	gotkio "github.com/digisan/gotk/io"
	"github.com/digisan/go-generics/str"
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

	// if Trim & Split are both disabled, disable progress-bar anyway.
	progbar = cfg.Trim.Enabled || cfg.Split.Enabled

	const tempdir = "./tmp/"
	os.RemoveAll(tempdir)

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
		files, _, err := fd.WalkFileDir(inFolderAbs, cfg.WalkSubFolders)
		if err != nil {
			return err
		}
		bar = uip.AddBar(len(files))
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

			// ct.ForceSingleProc(true)
			ct.KeepCatHeaders(false)
			ct.KeepIgnCatHeaders(true)
			ct.StrictSchema(true)
			ct.Dir4NotSplittable(cfg.Split.IgnoreFolder)

			outFile := filepath.Join(cfg.Split.OutFolder, tailPath)
			outFolder := filepath.Dir(outFile)
			splitfiles, ignoredfiles, _ := ct.Split(path, outFolder, cfg.Split.Schema...)

			// trim columns also apply to split result if set
			if cfg.Trim.Enabled && cfg.TrimColAfterSplit {
				for _, sf := range splitfiles {
					ct.QueryFile(sf, false, cfg.Trim.Columns, '&', nil, sf)
				}
			}

			// find valid schema, empty content file to spread
			for _, ignf := range ignoredfiles {

				hdrs, n, _ := ct.FileInfo(ignf)
				if err != nil {
					log.Printf("%v @ %s", err, ignf)
					return err
				}

				if n == 0 && str.Superset(hdrs, cfg.Split.Schema) {
					emptycsv, err := os.ReadFile(ignf)
					if err != nil {
						log.Printf("%v @ %s", err, ignf)
						return err
					}

					// Trim Columns if needed
					rmHdrs := cfg.Split.Schema
					if cfg.Trim.Enabled && cfg.TrimColAfterSplit {
						rmHdrs = str.MkSet(append(rmHdrs, cfg.Trim.Columns...)...)
					}
					var buf bytes.Buffer
					ct.Subset(emptycsv, false, rmHdrs, false, nil, io.Writer(&buf))
					emptycsv = buf.Bytes()

					mFileEmptyCSV[ignf] = emptycsv
				}

			}
		}

		if cfg.Trim.Enabled {
			// fmt.Printf("Trim Processing...: %v\n", path)

			outFolder := cfg.Trim.OutFolder

			// if trim output folder is identical to input folder, make a temp output, then overwrite the input
			if cfg.Trim.OutFolder == cfg.InFolder {
				outFolder = ""
				for i, path := range filepath.SplitList(cfg.Trim.OutFolder) {
					if i == 0 {
						outFolder = filepath.Join(outFolder, tempdir)
						continue
					}
					outFolder = filepath.Join(outFolder, path)
				}
			}

			outFile := filepath.Join(outFolder, tailPath)
			ct.QueryFile(path, false, cfg.Trim.Columns, '&', nil, outFile)
		}

		// -- progress bar 2 -- //
		if progbar {
			atomic.AddUint64(&procsize, 1)
			bar.Set(int(procsize))
			bar.Incr()
		}

		return nil

	}) // end of walk

	if err != nil {
		log.Fatalf("error walking the path %q: %v\n", inFolderAbs, err)
	}

	// if temp folder was created as Trim.OutFolder is the same as InFolder, use temp folder to replace input folder
	if fd.DirExists(tempdir) {
		os.RemoveAll(cfg.InFolder)
		os.Rename(tempdir, filepath.SplitList(cfg.InFolder)[0])
	}

	// spread all valid schema & empty csv to each split folder
	mOutRecord := make(map[string]struct{})
	if cfg.Split.Enabled {
		err = filepath.Walk(cfg.Split.OutFolder, func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) != ".csv" || info.IsDir() {
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
				if strings.HasSuffix(outdir, "/"+emptyroot) || strings.HasSuffix(outdir, "\\"+emptyroot) {
					gotkio.MustWriteFile(filepath.Join(outpath, emptyfile), csv)
				}
			}
		}
	}

	watched := []string{}
	mDirBase := make(map[string][]string)

	for i, m := range cfg.Merge {
		cfg.Merge[i].Schema = str.MkSet(append(m.Schema, m.MergedName)...)
		watched = str.Union(watched, cfg.Merge[i].Schema)
	}
	watched = str.MkSet(watched...)

	_, dirs, err := fd.WalkFileDir(cfg.Split.OutFolder, true)
	if err != nil {
		log.Fatalf("error walking FileDir %q: %v\n", cfg.Split.OutFolder, err)
	}

	for _, dir := range dirs {
		base := filepath.Base(dir)
		dir1 := filepath.Dir(dir)
		if str.In(base, watched...) {
			mDirBase[dir1] = append(mDirBase[dir1], base)
		}
	}

	onConflict := func(existing []byte, incoming []byte) (overwrite bool, overwriteData []byte) {
		iLF := bytes.Index(incoming, []byte{'\n'})
		return true, append(existing, incoming[iLF:]...)
	}

	for dir1, folders := range mDirBase {
		for _, folder := range folders {
			dir := filepath.Join(dir1, folder)
			for _, m := range cfg.Merge {
				if m.Enabled {
					temp := filepath.Join(dir1, m.MergedName+"#")
					// merged := filepath.Join(dir1, m.MergedName)
					for _, s := range m.Schema {
						if s == folder {
							// fmt.Println(dir, "=>", merged)
							fd.MergeDir(temp, true, onConflict, dir)
						}
					}
				}
			}
		}
	}

	_, dirs, err = fd.WalkFileDir(cfg.Split.OutFolder, true)
	for _, dir := range dirs {
		if strings.HasSuffix(dir, "#") {
			os.Rename(dir, dir[:len(dir)-1])
		}
	}

	return err
}
