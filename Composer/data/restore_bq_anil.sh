#!/bin/bash
sourceProject=$1
targetProject=$2
datasetPrefix='snapshot_'
function argumentCheck() {
    if [[ $sourceProject == *":"* && $sourceProject == *"."* ]]; then
        projectwithdataset=''
        tables=()
        IFS='.'
        index=0
        for item in $sourceProject; do
            if [ $index == 0 ]; then
                projectwithdataset=$item
            else
                tables+=($item)
                restoreTable $projectwithdataset $item
            fi
            index=$((index + 1))
        done
    elif [[ $sourceProject == *":"* && $sourceProject != *"."* ]]; then
        str=$sourceProject
        delimiter=':'
        s=$str$delimiter
        array=()
        while [[ $s ]]; do
            array+=("${s%%"$delimiter"*}")
            s=${s#*"$delimiter"}
        done
        declare -p array
        projectname=${array[0]}
        datset=${array[1]}
        if [[ $dataset == $datasetPrefix* ]]; then
            taragetDataset=${dataset//$datasetPrefix/}
            location=$(bq show --format=json ${projectname}:$dataset | grep -o '"location":"[^"]*' | grep -o '[^"]*$')
            msg=$(bq --location=$location mk --dataset --description="Dataset for snapshot" ${targetProject}:${taragetDataset})
            echo $msg
        fi
    else
        echo "Project"
    fi
}

restoreTable() {
    projectDataset=$1
    tbl=$2
    str=$projectDataset
    delimiter=':'
    s=$str$delimiter
    array=()
    while [[ $s ]]; do
        array+=("${s%%"$delimiter"*}")
        s=${s#*"$delimiter"}
    done
    declare -p array
    dataset=${array[1]}
    taragetDataset=${dataset//$datasetPrefix/}
    echo ${array[0]}:$dataset.$tbl $targetProject:$taragetDataset.$tbl
    msg=$(bq cp --restore --no_clobber ${array[0]}:$dataset.$tbl $targetProject:$taragetDataset.$tbl)
    echo $msg
    if [[ $msg == *"successfully restored"* ]]; then
        echo "Successfully snapshotted for ${array[0]}:${dataset}.${tbl} table."
    else
        echo "Warning: Snapshot is not created for table ${array[0]}:${dataset}.${tbl}"
    fi
}

argumentCheck
