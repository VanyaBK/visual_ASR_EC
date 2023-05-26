#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "$0" )" && pwd )"
cd ${THIS_DIR}

ARGS=""

until [[ -z "$1" ]]
do
    case $1 in
        -query)
            shift; QUERY=$1;
            shift;;
        -output)
            shift; OUTPUT=$1;
            shift;;
        -interval)
            shift; INTERVAL=$1;
            shift;;
        *)
            ARGS="${ARGS} $1";
            shift;
    esac
done

if [[ ! ${OUTPUT} ]]; then
    cur_date=`date +"%Y%m%d"`
    OUTPUT=${THIS_DIR}/outputs/${cur_date}
fi

if [[ ${OUTPUT} == hdfs* ]]; then
    hadoop fs -mkdir -p ${OUTPUT}
else
    mkdir -p ${OUTPUT}
fi

if [[ ! ${INTERVAL} ]]; then
    INTERVAL=300
fi

if [[ ${QUERY} == http* ]]; then
    wget ${QUERY} -O query.txt
elif [[ ${QUERY} == hdfs* ]]; then
    hadoop fs -get ${QUERY} query.txt
else
    cp ${QUERY} query.txt
fi

python3 src/downloader.py ${ARGS} \
    -o `pwd youtube_files` \
    --sync-interval ${INTERVAL} \
    --remote-output ${OUTPUT} || exit -1
