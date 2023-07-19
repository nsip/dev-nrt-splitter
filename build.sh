#!/bin/bash

# rm -f ./go.sum
# go get ./...

R=`tput setaf 1`
G=`tput setaf 2`
Y=`tput setaf 3`
W=`tput sgr0`

ORIPATH=`pwd`

cd ./cmd && ./build.sh 
echo "${G}NRT_SPLITTER BUILT${W}"

cd "$ORIPATH"

cp -rf ./data ./build
cd ./build/data
unzip -q system_reports.zip 
rm system_reports.zip
echo "${Y}test reports are now in the /build/data${Y}"

cd "$ORIPATH"
