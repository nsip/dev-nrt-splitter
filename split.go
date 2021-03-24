package splitter

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	csvtool "github.com/cdutwhu/csv-tool"
	"github.com/nsip/dev-nrt-splitter/config"
)

// NrtSplit :
func NrtSplit(configurations ...string) error {
	defer os.RemoveAll("./tempcsv")

	cfg := config.GetConfig(configurations...)
	fmt.Println(cfg.InFolder)
	inDirAbs, err := filepath.Abs(cfg.InFolder)
	if err != nil {
		return err
	}

	err = filepath.Walk(cfg.InFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf("Error [%v] at a path [%q], Check your config.toml [InFolder] \n", err, path)
			return err
		}

		var (
			fPath = path
			fDir  = filepath.Dir(path) + "/"
			fName = info.Name()
			fExt  = filepath.Ext(path)
		)

		if info.IsDir() || fExt != ".csv" {
			return nil
		}

		if !cfg.WalkSubFolders {
			fDirAbs, err := filepath.Abs(fDir)
			if err != nil {
				log.Fatalf("Error when walk through abs %s", cfg.InFolder)
			}
			if inDirAbs != fDirAbs {
				return nil
			}
		}

		if cfg.Trim.Enabled {
			fmt.Printf("Trim Processing...: %v\n", fPath)
			csvtool.Query(fPath, false, cfg.Trim.Columns, '&', nil, cfg.Trim.OutFolder+fName, nil)
		}

		if cfg.Splitting.Enabled {
			fmt.Printf("Split Processing...: %v\n", fPath)
			csvtool.Split(fPath, cfg.Splitting.OutFolder, false, cfg.Splitting.Schema...)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("error walking the path %q: %v\n", cfg.InFolder, err)
	}

	return err
}
