package main

import (
	"testing"
)

func Test_main(t *testing.T) {
	tests := []struct {
		name string
	}{
		// TODO: Add test cases.
		{
			name: "OK",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			main()
		})
	}
}

/*
func TestWalk(t *testing.T) {
	filepath.WalkDir("../", func(path string, d fs.DirEntry, err error) error {
		fmt.Printf("%v\n", path)

		tail := strs.TrimHeadToFirst(path, "../")
		dst := filepath.Join("inFolder", tail)
		fmt.Printf("%v\n\n", dst)

		return nil
	})
}
*/
