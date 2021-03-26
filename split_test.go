package splitter

import (
	"testing"
)

func TestNrtSplit(t *testing.T) {
	type args struct {
		configurations []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{
			name: "OK",
			args: args{
				configurations: []string{"./config/config.toml"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := NrtSplit(tt.args.configurations...); (err != nil) != tt.wantErr {
				t.Errorf("NrtSplit() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
