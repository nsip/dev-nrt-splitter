#!/bin/bash
set -e

R=`tput setaf 1`
G=`tput setaf 2`
Y=`tput setaf 3`
W=`tput sgr0`

BUILDDIR=../build
OUT=report_splitter

rm -rf $BUILDDIR
mkdir -p $BUILDDIR

GOARCH=amd64
LDFLAGS="-s -w"

# For Docker, one build below for linux64 is enough.
OUTPATH=$BUILDDIR/linux64/
mkdir -p $OUTPATH
CGO_ENABLED=0 GOOS="linux" GOARCH="$GOARCH" go build -ldflags="$LDFLAGS" -o $OUT
mv $OUT $OUTPATH
cp ../config/config.toml $OUTPATH'config.toml'
echo "${G}${OUT}(linux64) built${W}"

OUTPATH=$BUILDDIR/win64/
mkdir -p $OUTPATH
CGO_ENABLED=0 GOOS="windows" GOARCH="$GOARCH" go build -ldflags="$LDFLAGS" -o $OUT.exe
mv $OUT.exe $OUTPATH
cp ../config/config.toml $OUTPATH'config.toml'
echo "${G}${OUT}(win64) built${W}"

OUTPATH=$BUILDDIR/mac/
mkdir -p $OUTPATH
CGO_ENABLED=0 GOOS="darwin" GOARCH="$GOARCH" go build -ldflags="$LDFLAGS" -o $OUT
mv $OUT $OUTPATH
cp ../config/config.toml $OUTPATH'config.toml'
echo "${G}${OUT}(mac) built${W}"
