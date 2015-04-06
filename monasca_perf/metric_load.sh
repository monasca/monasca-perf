if [ -z $1 ]; then
    COUNTER=1
else
    COUNTER=$1
fi

if [ -z $2 ]; then
    STEP=1
else
    STEP=$2
fi

while [ "$?" = "0" ]; do
    python metric_perf.py $COUNTER
    if [ "$?" = "1" ]; then
        break
    fi
    let COUNTER=COUNTER+$STEP
done