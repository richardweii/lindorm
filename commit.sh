#!/bin/bash

TMP="lindorm-tsdb-contest-cpp"
OUTPUT="commit.zip"
rm ${OUTPUT} -rf

mkdir lindorm-tsdb-contest-cpp -p
cp source lindorm-tsdb-contest-cpp -r
cp include lindorm-tsdb-contest-cpp -r

SRC="lindorm-tsdb-contest-cpp"

if [ "$1" == "debug" ]; then
echo "Debug Mode"
touch debug
SRC="lindorm-tsdb-contest-cpp debug"
else 
echo "Commit Mode"
fi

zip -qr ${OUTPUT} ${SRC}
rm ${TMP} -rf
rm debug -f