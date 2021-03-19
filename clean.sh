#!/bin/bash

rm -rf ./in
rm -rf ./out
rm -rf ./tempcsv
rm -rf ./cmd/tempcsv
rm -rf ./build

# delete all binary files
find . -type f -executable -exec sh -c "file -i '{}' | grep -q 'x-executable; charset=binary'" \; -print | xargs rm -f
for f in $(find ./ -name '*.log' -or -name '*.doc'); do rm $f; done