#!/bin/bash

rm -f ./go.sum
go get ./...

R=`tput setaf 1`
G=`tput setaf 2`
Y=`tput setaf 3`
W=`tput sgr0`

oripath=`pwd`

cd ./cmd && ./build.sh && cd "$oripath"
echo "${G}NRT_SPLITTER BUILDING DONE${W}"