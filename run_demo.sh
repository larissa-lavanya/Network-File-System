#!/bin/bash
# run_demo.sh - starts 1 NM + 3 SS on localhost with sample data (stop with: ./run_demo.sh stop)
cd "$(dirname "$0")"
if [ "$1" = "stop" ]; then pkill -f ./naming_server; pkill -f ./storage_server; echo stopped; exit; fi
make -s || exit 1
rm -rf demo && mkdir -p demo/ss1/docs demo/ss2/music demo/ss3/misc
echo "hello from ss1" > demo/ss1/docs/a.txt; echo "notes on ss2" > demo/ss2/b.txt; echo "ss3 file" > demo/ss3/misc/c.txt
head -c 20000 /dev/urandom > demo/ss2/music/song.mp3   # fake audio file for STREAM demo
./naming_server -p 5000 > demo/nm.out 2>&1 &
sleep 1
for i in 1 2 3; do ./storage_server -n 127.0.0.1 -p 5000 -m $((6000+i)) -c $((7000+i)) -d demo/ss$i -i 127.0.0.1 > demo/ss$i.out 2>&1 & sleep 0.5; done
echo "NM on 127.0.0.1:5000, SS1-3 running. Try: ./client -n 127.0.0.1 -p 5000"
