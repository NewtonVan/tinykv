#!/bin/zsh
for ((i=1;i<=70;i++));
do
    echo "ROUND $i";
    make keynotinregion > ./out/out-key-$i.txt;
done