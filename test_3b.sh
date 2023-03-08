#!/bin/zsh
for ((i=1;i<=100;i++));
do
    echo "ROUND $i";
    make project3b > ./out/out-3b-$i.txt;
done