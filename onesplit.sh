#!/bin/zsh
for ((i=1;i<=30;i++));
do
    echo "ROUND $i";
    make onesplit3b > ./out/out-onesplit3b-$i.txt;
done
