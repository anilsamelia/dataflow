#!/bin/bash
################################################################################################################################################################################
#Script Name	    : bq_table_restore.sh
#Description	    : Script used to restore the bigquery datasets and tables
#Solution Architect : Anil Kumar
#Author       	    : Anil Kumar
#Email         	    : anilsamelia7@gmail.com
#Args               : --sourceProject=[value] --targetProject=[value] --datasets=[dataset1,dataset2] --tables=[dataset.table1,dataset.table2] --excludeDatasets=[dataset3,dataset4]
#                     --excludeTables=[dataset.table1,dataset.table2] --maxDatasets=800 --maxTablesPerDataset=200
#
#
################################################################################################################################################################################

input_Of_args=$@
sep='--'
list_of_args="${input_Of_args//$sep/$'\n'}"
declare -A args_map
#dataset_prefix='snapshot_'
success_table_count=0
total_table_count=0
total_dataset=0
created_dataset_count=0
dataset_table_count=0
dataset_source_list=()
include_datset_tbl_ary=()
exclude_datset_tbl_ary=()
is_skip_table="false"

function getArgs() {
    for arg in $list_of_args; do
        if [[ $arg == *"sourceProject"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['sourceProject']=${split[1]}
        elif [[ $arg == *"targetProject"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['targetProject']=${split[1]}
         elif [[ $arg == *"datasetPrefix"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['datasetPrefix']=${split[1]}
            dataset_prefix=${args_map[datasetPrefix]//-/_}'__'
            args_map['datasetPrefix']=dataset_prefix
            #dataset_prefix=${args_map[sourceProject]//-/_}'__'            
        elif [[ $arg == *"datasets"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['datasets']=${split[1]}
        elif [[ $arg == *"tables"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['tables']=${split[1]}
        elif [[ $arg == *"excludeDatasets"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['excludeDatasets']=${split[1]}
        elif [[ $arg == *"excludeTables"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['excludeTables']=${split[1]}
        elif [[ $arg == *"maxDatasets"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['maxDatasets']=${split[1]}
        elif [[ $arg == *"maxTablesPerDataset"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['maxTablesPerDataset']=${split[1]}
        elif [[ $arg == *"overrideTables"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['overrideTables']=${split[1]}
        elif [[ $arg == *"headless"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            if [[ ${split[1]} == 'false' ]]; then
                args_map['displayLogs'] ='false'
            else
                args_map['displayLogs']='headless'
            fi            
        elif [[ $arg == *"help"* ]]; then
            printHelp
            exit 100
        else
            ERROR "You have entered wrong parameter: $arg"
            echo 'Please check given below parameter names with spelling'
            echo '--sourceProject'
            echo '--datasets'
            echo '--targetProject'
            echo '--tables'
            echo '--excludeDatasets'
            echo '--excludeTables'
            echo '--overrideTables'
            echo '--headless'
            exit 100
        fi
    done
}

printHelp() {
    printf "\n --sourceProject: Project ID which have snapshoted datasets. (Mandatory parameter)\n"
    printf "\n --targetProject: Project ID . Where you want restore.  (Mandatory parameter)\n"
    printf "\n --datasets: Specific dataset's tables You want to  restore.  (Optional)\n"
    printf "\n --tables: Specific table or tables You want to restore. (Optional)\n"
    printf "\n --excludeDatasets: If you want to skip the datasets. (Optional)\n"
    printf "\n --excludeTables: If you want to skip tables. (Optional)\n"
    printf "\n --maxDatasets: Max dataset in the project  default is 100. (Optional)\n"
    printf "\n --maxTablesPerDataset: Max tables per dataset by default is 100. (Optional)\n "
    printf "\n --overrideTables: true|false. Default is false . Use to overide the exist table (Optional) \n"
    printf "\n --headless: true|false Default is false. If OS is headless value should be true . (Optional)\n"
}

ERROR() {
    local msg=$1
    if [[ ${args_map[displayLogs]} == "headless" ]]; then
        echo "[ERROR] ${msg}"
    else
        echo -e "\e[1;31m[ERROR]:\e[0m ${msg}"
    fi
}
WARNING() {
    local msg=$1
    if [[ ${args_map[displayLogs]} == "headless" ]]; then
        echo "[WARNING] ${msg}"
    else
        echo -e "\e[1;33m[WARNING]:\e[0m ${msg}"
    fi
}

INFO() {
    local msg=$1
    if [[ ${args_map[displayLogs]} == "headless" ]]; then
        echo "[INFO] ${msg}"
    else
        echo -e "\e[1;34m[INFO]: \e[0m ${msg}"
    fi
}


vaildationCheck() {
    if [ ! -n "${args_map[sourceProject]}" ]; then
        ERROR "Source project Id is missed please pass --sourceProject={projectid}."
        exit 100
    elif [ ! -n "${args_map[targetProject]}" ]; then
        ERROR "Target project Id is missed please pass --targetProject={projectid}."
        exit 100
    elif [ ! -n "${args_map[datasetPrefix]}" ]; then
        ERROR "Snapshot prefix is missed please pass --datasetPrefix={prefix}."
        exit 100        
    fi
    if [ ! -n "${args_map[maxDatasets]}" ]; then
        args_map['maxDatasets']=100
        WARNING "No --maxDatasets is set. Default maximum dataset limit is ${args_map[maxDatasets]} from project"
    else
        INFO "Max dataset per project is ${args_map[maxDatasets]}"
    fi
    if [ ! -n "${args_map[maxTablesPerDataset]}" ]; then
        args_map['maxTablesPerDataset']=100
        WARNING "No --maxTablesPerDataset is set. Default maximum table limit is ${args_map[maxTablesPerDataset]} tables from dataset"
    else
        INFO "Max dataset per project is ${args_map[maxDatasets]}"
    fi
    if [ ! -n "${args_map[overrideTables]}" ]; then
        args_map['overrideTables']='false'
    else
        if [[ ${args_map[overrideTables]} == 'true' ]]; then
            WARNING "If snapshot table already exist. Then script will override"
        fi
    fi
    #Pre-Check for Permission  
    gcloud projects describe  ${args_map[sourceProject]} ||  exit 100
    gcloud projects describe  ${args_map[targetProject]} ||  exit 100
}

function createRestoreDataset() {
    INFO 'Start creating dataset'
    local tempDatset=()
    tempDatset=($(echo "${dataset_source_list[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')) # Avoid duplicate
    unset dataset_source_list
    dataset_source_list=${tempDatset[@]}
    if [[ $SCENARIO == 'PROJECT' ]]; then
        local templist=$(bq ls --max_results ${args_map[maxDatasets]} ${args_map[sourceProject]}: | awk '(NR>2)')
        echo ${templist[@]}
        for item in ${templist[@]}; do
            if [[ $item == $dataset_prefix* ]]; then
                dataset_source_list+=("$item")
            fi
        done
    fi
    excludeDataset
    for dataset in ${dataset_source_list[@]}; do
        total_dataset=$((total_dataset + 1))
        if [[ $dataset == $dataset_prefix* ]]; then
            createdataset $dataset
            continue
        else
            WARNING "$dataset is not prefix with snapshot dataset. So continue without creating dataset"
        fi
    done
    if [[ $created_dataset_count == 0 ]]; then
        WARNING "No dataset has been created for ${args_map[sourceProject]}"
    else
        INFO "${created_dataset_count} / ${total_dataset} datasets has been created successfully"
    fi
    
}

createdataset() {
    local dataset=$1
    local location=$(bq show --format=json ${args_map[sourceProject]}:$dataset | grep -o '"location":"[^"]*' | grep -o '[^"]*$')
    dsName=${dataset//${dataset_prefix}/}
    local msg=$(bq --location=$location mk --dataset --description="Dataset for snapshot" ${args_map[targetProject]}:${dsName})
    if [[ $msg == *"successfully created"* ]]; then
        INFO "${msg}"
        created_dataset_count=$(($created_dataset_count + 1))
    else
        WARNING "${msg}"
    fi
}

function createtableRestore() {
    for dataset in ${dataset_source_list[@]}; do
        datasetotal_table_count=0
        dataset_table_count=0
        local listtbl=$(bq ls --max_results ${args_map[maxTablesPerDataset]} ${args_map[sourceProject]}:${dataset} | grep 'SNAPSHOT' | awk '{print $1}')
        for tbl in $listtbl; do
            checkTableExclude $dataset $tbl
            if [[ $is_skip_table == "false" ]]; then
                datasetotal_table_count=$(($datasetotal_table_count + 1))
                restoreTable $dataset $tbl
            else
                WARNING "Skip ${args_map[sourceProject]}:${dataset}.$tbl"
            fi
        done
       if [[ $dataset_table_count == 0 ]]; then
          WARNING "No table has been processed for ${args_map[sourceProject]}:${dataset}"
        else
          INFO "${dataset_table_count} / ${datasetotal_table_count} tables has been processed successfully for ${args_map[sourceProject]}:${dataset}"
        fi
    done
}

function restoreTable() {
    local dataset=$1
    local tbl=$2
    total_table_count=$((total_table_count + 1))
    dsName=${dataset//$dataset_prefix/}
    restore $dataset $tbl $dsName
    if [[ $msg == *"successfully"* ]]; then
        success_table_count=$((success_table_count + 1))
        dataset_table_count=$((dataset_table_count + 1))
        INFO "${msg}"
    elif [[ $msg == *"already exists"* ]]; then
        if [[ ${args_map[overrideTables]} == 'true' ]]; then
            WARNING "Start remove exist table ${args_map[targetProject]}:${dataset}.${tbl}"
            bq rm --f ${args_map[targetProject]}:${dsName}.${tbl}
            restore $dataset $tbl $dsName
            if [[ $msg == *"successfully"* ]]; then
                success_table_count=$((success_table_count + 1))
                dataset_table_count=$((dataset_table_count + 1))
                INFO "${msg}"
            else
                ERROR "${msg}"
            fi
        fi
    else
        WARNING "${msg}"
    fi
}

restore() {
    local dataset=$1
    local tbl=$2
    local dsName=$3
    unset msg
    msg=$(bq query --use_legacy_sql=false "CREATE TABLE \`${args_map[targetProject]}.${dsName}.${tbl}\` CLONE \`${args_map[sourceProject]}.${dataset}.${tbl}\`")
    if [[ $msg == *"successfully"* ]] |[[ $msg == *"Created"* ]]; then
        rdate=`date "+%F"`
        date_reset=$(bq update --set_label restore_on:${rdate} ${args_map[targetProject]}:${dsName}.${tbl})
    fi
}

excludeDataset() {
    local SAVEIFS=$IFS
    local IFS=','
    for dataset in ${args_map[excludeDatasets]}; do
        if [[ $dataset == $dataset_prefix* ]]; then
            dataset_source_list=("${dataset_source_list[@]/$dataset/}")
            INFO "Excluded Dataset:- ${args_map[sourceProject]}:$dataset"
        fi
    done
    IFS=$SAVEIFS
}

includeDatasets() {
    local SAVEIFS=$IFS
    local IFS=','
    for dataset in ${args_map[datasets]}; do
        local msg=$(bq ls ${args_map[sourceProject]}:${dataset})
        if [[ $msg == *"Not found: Dataset"* ]]; then
            ERROR "${msg}"
            exit 100
        fi
        dataset_source_list+=("$dataset")
        INFO "Included Dataset:- ${args_map[sourceProject]}:$dataset"
    done
    IFS=$SAVEIFS
}

checkTableExclude() {
    local dataset=$1
    local tbl=$2
    local data_tbl="${dataset}.${tbl}"
    is_skip_table="false"
    for datasettbl in ${exclude_datset_tbl_ary[@]}; do
        if [[ $datasettbl == $data_tbl ]]; then
            is_skip_table="true"
            break
        fi
    done
}

includeTables() {
    local SAVEIFS=$IFS
    local IFS=','
    for dataset_tbl in ${args_map[tables]}; do
        include_datset_tbl_ary+=("$dataset_tbl")
        INFO "Included Table:- ${args_map[sourceProject]}:$dataset_tbl"
    done
    IFS=$SAVEIFS
    for datasettbl in ${include_datset_tbl_ary[@]}; do
        dataset=${datasettbl//'.'*/}
        dataset_source_list+=("${dataset}")
    done
}

excludeTables() {
    local SAVEIFS=$IFS
    local IFS=','
    for dataset_tbl in ${args_map[excludeTables]}; do
        if [[ $dataset_tbl == $dataset_prefix* ]]; then
            exclude_datset_tbl_ary+=("$dataset_tbl")
        fi
        INFO "Excluded table:-  ${args_map[sourceProject]}:$dataset_tbl"
    done
    IFS=$SAVEIFS
}
######################################################################################### START  EXECUTION ########################################################################
getArgs
vaildationCheck
excludeTables
if [[ -z "${args_map[datasets]}" && -z "${args_map[tables]}" ]]; then
    SCENARIO='PROJECT'
    INFO "Start Restoration for all the datasets."
    createRestoreDataset
    createtableRestore
else
    SCENARIO='DATASET'
    INFO "Start Restoration for specific datasets."
    includeDatasets
    createRestoreDataset
    createtableRestore
    if [ -n "${args_map[tables]}" ]; then
        SCENARIO='TABLES'
        INFO "Start Restoration for specific tables."
        unset dataset_source_list
        includeTables
        createRestoreDataset
        for datasettbl in ${include_datset_tbl_ary[@]}; do
            dataset=${datasettbl//'.'*/}
            tbl=${datasettbl//*'.'/}
            restoreTable ${dataset} ${tbl}
        done
    fi
fi

INFO "${success_table_count} / ${total_table_count} tables has been processed successfully for ${args_map[sourceProject]}"
######################################################################################### END OF EXECUTION ###########################################################################