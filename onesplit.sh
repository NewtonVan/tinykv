#!/bin/zsh
for ((i=1;i<=20;i++));
do
    echo "ROUND $i";
    make OneSplit3b > ./out/out-one-$i.txt;
done
