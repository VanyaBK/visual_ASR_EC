#!/usr/bin/env bash

THIS_DIR="$( cd "$( dirname "$0" )" && pwd )"
cd ${THIS_DIR}

ARGS=""

until [[ -z "$1" ]]
do
    case $1 in
        -keyword)
            shift; KEYWORD=$1;
            shift;;
        -output)
            shift; OUTPUT=$1;
            shift;;
        -size)
            shift; SIZE=$1;
            shift;;
        -format)
            shift; FORMAT=$1;
            shift;;
        *)
            ARGS="${ARGS} $1";
            shift;
    esac
done

if [[ ! ${OUTPUT} ]]; then
    cur_date=`date +"%Y%m%d"`.json
    OUTPUT=${THIS_DIR}/outputs/${cur_date}
fi

if [[ ! ${KEYWORD} ]]; then
    echo "Please provide keyword for searching!"
    exit -10000
fi

if [[ ! ${SIZE} ]]; then
    SIZE=50
fi

if [[ ! ${FORMAT} ]]; then
    FORMAT="url_title"
fi

mkdir -p `dirname ${OUTPUT}`

python3 src/youtube_search.py \
    --query ${KEYWORD} \
    --minimum-number ${SIZE} \
    --format ${FORMAT} \
    --output-path ${OUTPUT} ${ARGS} || exit -1
