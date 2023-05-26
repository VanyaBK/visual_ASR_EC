#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "$0" )" && pwd )"
cd ${THIS_DIR}

ARGS=""

until [[ -z "$1" ]]
do
    case $1 in
        -url_file)
            shift; URL_FILE=$1;
            shift;;
        -output)
            shift; OUTPUT=$1;
            shift;;
        -format)
            shift; FMT=$1;
            shift;;
        -interval)
            shift; INTERVAL=$1;
            shift;;
        -num_workers)
            shift; N_WORKER=$1;
            shift;;
        -download_subtitle)
            shift;
            DL_SUB="--download-subtitle";;
        -download_info)
            shift;
            DL_INFO="--download-info";;
        -pack_mode)
            shift;
            PACK_MODE=$1;
            shift;;
        *)
            ARGS="${ARGS} $1";
            shift;
    esac
done

if [[ ! ${FMT} ]]; then
    FMT=mp4
fi

if [[ ! ${INTERVAL} ]]; then
    INTERVAL=60
fi

if [[ ! ${OUTPUT} ]]; then
    cur_date=`date +"%Y%m%d"`
    OUTPUT=${THIS_DIR}/outputs/${cur_date}
fi

if [[ ! ${PACK_MODE} ]]; then
    PACK_MODE="tar"
fi

if [[ ${URL_FILE} == http* ]]; then
    wget ${URL_FILE} -O url_file.txt
elif [[ ${URL_FILE} == hdfs* ]]; then
    hadoop fs -get ${URL_FILE} url_file.txt
else
    cp ${URL_FILE} url_file.txt
fi


echo "begin download and sync videos to ${OUTPUT}"
python3 src/download_urls.py \
    --url-file url_file \
    --format ${FMT} \
    --output ${THIS_DIR}/youtube_files \
    --remote-output ${OUTPUT} \
    --sync-interval ${INTERVAL} \
    --num-workers ${N_WORKER} \
    ${DL_SUB} ${DL_INFO} --pack-mode ${PACK_MODE} ${ARGS} || exit -1
