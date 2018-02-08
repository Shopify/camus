CHECK_DATE=$1   # e.g. `date -d "yesterday" '+%Y/%m/%d'`  # e.g. 2017/01/01

TIME_NOW=`date +%s`
EXIT_CODE=0

while read TOPIC; do
    CHECK_DIR=${TOPIC//_/.}
    echo "Reconciling ${CHECK_DIR}/${CHECK_DATE}"

    TMP_RECON_STORE=/tmp/camus/reconciler/${TIME_NOW}
    CHECK_PATH="data/raw/kafka/${CHECK_DIR}/${CHECK_DATE}"
    mkdir -p ${TMP_RECON_STORE}

    FILES_ON_HDFS="${TMP_RECON_STORE}/${CHECK_DIR}_on_hdfs.txt"
    HADOOP_USER_NAME=deploy hadoop fs -ls -R -C hdfs://hadoop-production/${CHECK_PATH} | grep .gz | sort | uniq > ${FILES_ON_HDFS}
    sed -i -e 's+hdfs://hadoop-production++g' ${FILES_ON_HDFS}

    FILES_ON_GCS="${TMP_RECON_STORE}/${CHECK_DIR}_on_gcs.txt"
    HADOOP_USER_NAME=deploy hadoop fs -ls -R -C gs://raw-kafka/${CHECK_PATH} | grep .gz | sort | uniq > ${FILES_ON_GCS}
    sed -i -e 's+gs://raw-kafka++g' ${FILES_ON_GCS}

    MISSING_FILES="${TMP_RECON_STORE}/${CHECK_DIR}_missing.txt"
    comm -23 ${FILES_ON_HDFS} ${FILES_ON_GCS} > ${MISSING_FILES}
    MISSING_COUNT=`cat ${MISSING_FILES} | wc -l`

    if [ ${MISSING_COUNT} != 0 ]; then
        echo "Could not reconcile topic ${CHECK_PATH}, found missing files:"
        cat ${MISSING_FILES}
        EXIT_CODE=1
    else
        echo "Successfully reconciled ${CHECK_PATH}"
    fi

done < upload_topics_to_gcs

# report failure to Datadog
echo "Camus.UploadToGCSReconciliation.failure:${EXIT_CODE}|g" | nc -w 1 -u localhost 8125

exit ${EXIT_CODE}
