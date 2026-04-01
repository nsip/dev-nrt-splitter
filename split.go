package splitter

import (
	"bytes"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	. "github.com/digisan/go-generics"
	fd "github.com/digisan/gotk/file-dir"
	"github.com/digisan/gotk/strs"
	lk "github.com/digisan/logkit"
	"github.com/gosuri/uiprogress"
	"github.com/gosuri/uiprogress/util/strutil"
	ct "github.com/nsip/csv-tool"
	qry "github.com/nsip/csv-tool/query"
	spl "github.com/nsip/csv-tool/split"
	spl2 "github.com/nsip/csv-tool/split2"
)

func EnableProgBar(enable bool) {
	progBar = enable
}

// NrtSplit :
func NrtSplit(configurations ...string) (err error) {

	// fit toml configuration for 'Trim', 'Split' etc
	setConfig(configurations...)

	// by default, if Trim & Split are both disabled, disable progress-bar anyway.
	EnableProgBar(enableTrim || enableSplit)

	// prepare a temporary dir for trim & split
	const tempDir = "./ts_tmp/"
	if err = os.RemoveAll(tempDir); err != nil {
		return err
	}

	// -- progress bar 1 -- //
	if progBar {
		uip = uiprogress.New()
		defer uip.Stop()
		uip.Start()
		files, _, err := fd.WalkFileDir(inFolderAbs, goSubFolder)
		if err != nil {
			lk.WarnOnErr("%v", err)
			return err
		}
		bar = uip.AddBar(len(files))
		bar.AppendCompleted().PrependElapsed()
		bar.PrependFunc(func(b *uiprogress.Bar) string {
			return strutil.Resize(" Trimming & Splitting...:", 35)
		})
	}

	mFileEmptyCSV := make(map[string][]byte)

	err = filepath.Walk(inFolderAbs, func(fPath string, info os.FileInfo, err error) error {

		lk.FailOnErr("error [%v] at a path [%q], check your config.toml [InFolder] \n", err, fPath)

		// only process csv file
		if info.IsDir() || filepath.Ext(fPath) != ".csv" {
			return nil
		}

		// if NOT goSubFolder, if jump into deeper, then return
		if !goSubFolder {
			fDirAbs, err := filepath.Abs(filepath.Dir(fPath))
			lk.FailOnErr("Error when walk through abs %s, @%v", inFolderAbs, err)
			if inFolderAbs != fDirAbs {
				return nil
			}
		}

		// trim inFolder Path for each file path, only keep 'filename.csv' or '/sub/filename.csv'
		tailPath := fPath[len(inFolderAbs):]

		// Split first
		if enableSplit {
			// fmt.Printf("Split Processing...: %v\n", path)

			// split setting
			if bySplit2 {
				spl2.RmSchemaCol(true)                      // after splitting, remove those columns which are used by splitting
				spl2.RmSchemaColInIgn(false)                // keep all columns when a file cannot be split
				spl2.StrictSchema(true, ignoreFolder4Split) // strict for split, if doesn't meet Schema, then ignore this csv
			} else {
				// spl.ForceSglProc(true)
				spl.RmSchemaCol(true)
				spl.RmSchemaColInIgn(false)
				spl.StrictSchema(true, ignoreFolder4Split)
			}

			outFile := filepath.Join(out4Split, tailPath) // output file
			outFolder := filepath.Dir(outFile)            // output file's folder

			// do split
			var fPathsSplit, fPathsIgnore []string
			if bySplit2 {
				fPathsSplit, fPathsIgnore, err = spl2.Split(fPath, outFolder, splitSchema...)
			} else {
				fPathsSplit, fPathsIgnore, err = spl.Split(fPath, outFolder, splitSchema...)
			}
			if err != nil {
				// A split error on one file must not abort the whole run; warn and move on.
				lk.WarnOnErr("split failed for [%s]: %v — skipping to next file", fPath, err)
				return nil
			}

			// trim columns also apply to split result if set
			if enableTrim && trimColAfterSplit {
				for _, splitPath := range fPathsSplit {
					if ok, trimCheckErr := ct.FileHeaderHasAny(splitPath, trimCols...); trimCheckErr == nil && ok {
						// Write trim output to a temp path; replace original only on success.
						// This prevents a QueryFile error from leaving the split file empty.
						tmpPath := splitPath + ".trim_tmp"
						if trimErr := qry.QueryFile(splitPath, false, trimCols, '&', nil, tmpPath); trimErr != nil {
							lk.WarnOnErr("trim-after-split failed for [%s]: %v — leaving file untrimmed", splitPath, trimErr)
							os.Remove(tmpPath) // remove any empty/partial temp, leave original intact
						} else if renameErr := os.Rename(tmpPath, splitPath); renameErr != nil {
							lk.WarnOnErr("could not replace trimmed split file [%s]: %v — leaving file untrimmed", splitPath, renameErr)
							os.Remove(tmpPath)
						}
					}
				}
			}

			// find schema is valid, but empty content file to spread in future
			for _, fPath := range fPathsIgnore {

				hdr, n, err := ct.FileInfo(fPath)
				if err != nil {
					lk.Warn("%v @ %s", err, fPath)
					return err
				}

				if n == 0 && IsSuper(hdr, splitSchema) {
					emptyCsv, err := os.ReadFile(fPath)
					if err != nil {
						lk.Warn("%v @ %s", err, fPath)
						return err
					}

					// Trim Columns if needed
					rmHdr := splitSchema
					if enableTrim && trimColAfterSplit {
						rmHdr = Settify(append(rmHdr, trimCols...)...)
					}
					var buf bytes.Buffer
					qry.Subset(emptyCsv, false, rmHdr, false, nil, io.Writer(&buf))
					emptyCsv = buf.Bytes()

					mFileEmptyCSV[fPath] = emptyCsv
				}
			}
		}

		if enableTrim {
			lk.Log("Trim Processing: %v", fPath)

			if ok, trimCheckErr := ct.FileHeaderHasAny(fPath, trimCols...); trimCheckErr == nil && ok {
				outFolder := out4Trim
				// if trim output folder is identical to original input folder, make a temp output, then overwrite the input
				if out4Trim == inFolder {
					outFolder = tempDir
				}
				outFile := filepath.Join(outFolder, tailPath)

				// pre-truncate to prevent stale tail content on re-runs
				// (QueryFile in digisan/csv-tool opens without O_TRUNC)
				if fd.FileExists(outFile) {
					if truncErr := os.Truncate(outFile, 0); truncErr != nil {
						lk.WarnOnErr("could not truncate existing trim output [%s]: %v", outFile, truncErr)
					}
				}

				if trimErr := qry.QueryFile(fPath, false, trimCols, '&', nil, outFile); trimErr != nil {
					lk.WarnOnErr("trim failed for [%s]: %v — copying untrimmed file to output as fallback", fPath, trimErr)
					fd.MustCreateDir(filepath.Dir(outFile))
					func() {
						srcF, openErr := os.Open(fPath)
						if openErr != nil {
							lk.WarnOnErr("fallback: could not open source [%s]: %v", fPath, openErr)
							return
						}
						defer srcF.Close()
						dstF, createErr := os.Create(outFile)
						if createErr != nil {
							lk.WarnOnErr("fallback: could not create output [%s]: %v", outFile, createErr)
							return
						}
						defer dstF.Close()
						if _, copyErr := io.Copy(dstF, srcF); copyErr != nil {
							lk.WarnOnErr("fallback: copy failed [%s] -> [%s]: %v", fPath, outFile, copyErr)
						}
					}()
				} else {
					lk.Log("Done Trim Processing: %v", fPath)
				}
			}
		}

		// -- progress bar 2 -- //
		if progBar {
			atomic.AddUint64(&procSize, 1)
			bar.Set(int(procSize))
			bar.Incr()
		}

		return nil

	}) // end of walk

	lk.FailOnErr("error walking the path %q: %v\n", inFolderAbs, err)

	// if temp folder was created as Trim.OutFolder is the same as InFolder, use temp folder to replace input folder
	if fd.DirExists(tempDir) {

		// copy each file to inFolder, then delete tempDir
		err := filepath.WalkDir(tempDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				return nil
			}

			srcF, err := os.Open(path)
			if err != nil {
				lk.Warn("%v (%v)", err, path)
				return err
			}
			defer srcF.Close()

			subPath := strs.TrimHeadToFirst(path, filepath.Clean(tempDir))
			// lk.Log("%v -- %v -- %v", path, tempDir, subPath)
			dst := filepath.Join(inFolder, subPath)
			// lk.Log("%v", dst)

			dstF, err := os.Create(dst)
			if err != nil {
				lk.Warn("%v (%v)", err, dst)
				return err
			}
			defer dstF.Close()

			if _, err := io.Copy(dstF, srcF); err != nil {
				lk.Warn("%v (%v) (%v)", err, path, dst)
				return err
			}

			return nil
		})

		if err == nil {
			lk.Log("removing temp dir [%v]", tempDir)
			lk.WarnOnErr("%v", os.RemoveAll(tempDir))
		}

		// cause files missing issue !!!
		// os.RemoveAll(inFolder)
		// os.Rename(tempDir, inFolder)
	}

	// spread all valid schema & empty csv to each split folder
	mOutRecord := make(map[string]struct{})
	if enableSplit {
		err = filepath.Walk(out4Split, func(path string, info os.FileInfo, err error) error {
			if filepath.Ext(path) != ".csv" || info.IsDir() {
				return nil
			}
			mOutRecord[filepath.Dir(path)] = struct{}{}
			return nil
		})

		lk.WarnOnErr("error walking the path %q: %v\n", inFolderAbs, err)

		nSchema := len(splitSchema)
		for outPath := range mOutRecord {
			for emptyPath, csv := range mFileEmptyCSV {
				emptyRoot := filepath.Base(filepath.Dir(emptyPath))
				emptyFile := filepath.Base(emptyPath)
				outDir := outPath
				for i := 0; i < nSchema; i++ {
					outDir = filepath.Dir(outDir)
				}
				if strings.HasSuffix(outDir, "/"+emptyRoot) || strings.HasSuffix(outDir, "\\"+emptyRoot) {
					fd.MustWriteFile(filepath.Join(outPath, emptyFile), csv)
				}
			}
		}
	}

	watched := []string{}
	for _, m := range merges {
		schema := AnysToTypes[string](m["Schema"].([]any))
		watched = append(watched, schema...)
	}
	watched = Settify(watched...)

	_, dirs4split, err := fd.WalkFileDir(out4Split, true)
	if err != nil {
		if !enableSplit {
			if _, err = os.Stat(out4Split); os.IsNotExist(err) {
				err = nil
			}
		}
	}
	if err != nil {
		return err
	}

	mDirBase := make(map[string][]string)
	for _, dir := range dirs4split {
		base := filepath.Base(dir)
		dir1 := filepath.Dir(dir)
		if In(base, watched...) {
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
			for _, m := range merges {
				if m["Enabled"].(bool) {
					temp := filepath.Join(dir1, m["MergedName"].(string)+"#")
					// merged := filepath.Join(dir1, m.MergedName)
					schema := AnysToTypes[string](m["Schema"].([]any))
					for _, s := range schema {
						if s == folder {
							// fmt.Println(dir, "=>", merged)
							fd.MergeDir(temp, true, onConflict, dir)
						}
					}
				}
			}
		}
	}

	err = nil
	if enableSplit {
		_, dirs4split, err = fd.WalkFileDir(out4Split, true)
		if err != nil {
			return err
		}

		for _, dir := range dirs4split {
			if strings.HasSuffix(dir, "#") {
				os.Rename(dir, dir[:len(dir)-1])
			}
		}
	}

	return err
}
