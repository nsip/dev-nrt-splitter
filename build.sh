#!/bin/bash

# rm -f ./go.sum
# go get ./...

R=`tput setaf 1`
G=`tput setaf 2`
Y=`tput setaf 3`
W=`tput sgr0`

ORIPATH=`pwd`

cd ./cmd && ./build.sh 
cd "$ORIPATH"

echo "${G}NRT_SPLITTER BUILT${W}"