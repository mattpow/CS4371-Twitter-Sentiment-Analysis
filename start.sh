#!/bin/bash
lsof -ti:9001 | xargs kill
echo "Press 'r' to restart, 'e' to exit"
echo "Starting python scripts..."
python stream.py &
(sleep 1; python spark.py) &
echo "Logging..."
while : ; do
  read -n 1 k <&1
  if [[ $k = r ]] ; then
    pkill -f stream.py
    pkill -f spark.py
    lsof -ti:9001 | xargs kill
    printf "\033c\n"
    ./$(basename $0) && exit
  fi
  if [[ $k = e ]] ; then
    pkill -f stream.py
    pkill -f spark.py
    lsof -ti:9001 | xargs kill
    exit 1
  fi
done