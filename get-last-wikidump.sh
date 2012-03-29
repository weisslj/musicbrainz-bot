LANG=$1
FILE=${LANG}wiki-latest-all-titles-in-ns0.gz

rm ${FILE}
wget http://dumps.wikimedia.org/${LANG}wiki/latest/${FILE}

gunzip ${FILE}
