#!/bin/bash

################################################################################################################################################################################
#Script Name	    : bq_table_snapshot.sh
#Description	    : Script used to create snapshot of all datasets ,specific dataset or specfic tables
#Solution Architect : Anil Kumar
#Author       	    : Anil Kumar
#Email         	    : anilsamelia7@gmail.com
#Args               : --sourceProject=[value] --targetProject=[value] --datasets=[dataset1,dataset2] --tables=[dataset.table1,dataset.table2] --excludeDatasets=[dataset3,dataset4]
#                     --excludeTables=[dataset.table1,dataset.table2] --retentionDays=[days] --maxDatasets=800 --maxTablesPerDataset=200
#
# *  sourceProject: Project ID which you want create snapshot. (Mandatory parameter)
# *  targetProject: Project ID .There you want create snapshot.  (Mandatory parameter)
# *  datasets: Specific dataset's tables You want to create snapshot.  (Optional)
# *  tables: Specific tables You want to create snapshot. (Optional)
# *  excludeDatasets: If you want to skip the datasets. (Optional)
# *  excludeTables: If you want to skip tables. (Optional)
# *  retentionDays: Expiration of snapshot default reatentin is 7 years. (Optional)
# *  maxDatasets:  Specifies the max number of datasets to return per read.  default is 100. (Optional)
# *  maxTablesPerDataset:  Specifies the max number of tables to return per read. default is 100. (Optional)
################################################################################################################################################################################

input_Of_args=$@
sep='--'
list_of_args="${input_Of_args//$sep/$'\n'}"
declare -A args_map
calc_retaintion=0
dataset_prefix='snapshot_'
success_table_count=0
total_snapshot_count=0
source_total_table=0
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
        elif [[ $arg == *"retentionDays"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['retentionDays']=${split[1]}
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
            ####
        elif [[ $arg == *"overrideSnapshot"* ]]; then
            SAVEIFS=$IFS
            IFS='='
            read -a split <<<"$arg"
            IFS=$SAVEIFS
            args_map['overrideSnapshot']=${split[1]}
            ######
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
            echo '--overrideSnapshot'
            echo '--headless'
            echo '--datasetPrefix'
            exit 100
        fi
    done

}

printHelp() {
    printf "\n --sourceProject: Project ID. Which is you want create snapshot. (Mandatory parameter)\n"
    printf "\n --targetProject: Project ID. Where snapshot will create.  (Mandatory parameter)\n"
    printf "\n --datasetPrefix: Prefix value. While snapshot dataset would created with given prefix value (Mandatory)\n"
    printf "\n --datasets: Specific dataset's tables you want create snapshot.  (Optional)\n"
    printf "\n --tables: Specific tables You want to create snapshot. (Optional)\n"
    printf "\n --excludeDatasets: If you want to skip the datasets. (Optional)\n"
    printf "\n --excludeTables: If you want to skip tables. (Optional)\n"
    printf "\n --retentionDays: Expiration of snapshot default reatentin is 7 years. (Optioal)\n"
    printf "\n --maxDatasets: Specifies the max number of datasets to return per read. default is 100. (Optional)\n"
    printf "\n --maxTablesPerDataset: Specifies the max number of tables to return per read. default is 100. (Optional)\n "
    printf "\n --overrideSnapshot: true|false  If you want to override the snapshot table. Default value is false. (Optional)\n"
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
    if [ ! -n "${args_map[retentionDays]}" ]; then
        calc_retaintion=$((86400 * 365 * 7))
        WARNING "No retention period set. Default retention period is 7 years"
    else
        calc_retaintion=$((86400 * ${args_map[retentionDays]}))
        INFO "Retention period set for ${args_map[retentionDays]} days"
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
    if [ ! -n "${args_map[overrideSnapshot]}" ]; then
        args_map['overrideSnapshot']='false'
    else
        if [[ ${args_map[overrideSnapshot]} == 'true' ]]; then
            WARNING "If snapshot table already exist. Then script will override"
        fi
    fi
    
   # dataset_prefix=${args_map[sourceProject]//-/_}'__'
    #Pre-Check for Permission  
    gcloud projects describe  ${args_map[sourceProject]} ||  exit 100
    gcloud projects describe  ${args_map[targetProject]} ||  exit 100
}

function createSnapshotDataset() {
    INFO "Start creating snapshot dataset"
    local tempDatset=()
    tempDatset=($(echo "${dataset_source_list[@]}" | tr ' ' '\n' | sort -u | tr '\n' ' ')) # Avoid duplicate
    unset dataset_source_list
    dataset_source_list=${tempDatset[@]}
    if [[ $SCENARIO == 'PROJECT' ]]; then
        INFO "SCENARIO $SCENARIO"
        dataset_source_list=$(bq ls --max_results ${args_map[maxDatasets]} ${args_map[sourceProject]}: | awk '(NR>2)')
        if [ ! -n "${dataset_source_list}" ]; then
            ERROR "No Dataset found: ${args_map[sourceProject]}"
            exit 100
        fi
    fi
    excludeDataset
    for dataset in ${dataset_source_list[@]}; do
        total_dataset=$((total_dataset + 1))
        if [[ $dataset == $dataset_prefix* ]]; then
            WARNING "$dataset is already exists. So continue without creating dataset"
            continue
        else
            (createdataset $dataset)
        fi
    done
    wait
    #if [[ $created_dataset_count == 0 ]]; then
    #       WARNING "No dataset has been created for ${args_map[sourceProject]}"
    #    else
    #      INFO "${created_dataset_count} / ${total_dataset} datasets has been created successfully"
    #   fi
}

createdataset() {
    local dataset=$1
    local location=$(bq show --format=json ${args_map[sourceProject]}:$dataset | grep -o '"location":"[^"]*' | grep -o '[^"]*$')
    local msg=$(bq --location=$location mk --dataset --description="Dataset for snapshot" ${args_map[targetProject]}:${dataset_prefix}${dataset})
    if [[ $msg == *"successfully created"* ]]; then
        INFO "${msg}"
        created_dataset_count=$(($created_dataset_count + 1))
    else
        if [[ $msg == *"already exists"* && ${args_map[overrideSnapshot]} == 'true' ]]; then
        [[ $msg == *"immutable"* || $msg == *"already exists"* ]]
            #WARNING "${msg}"
            bq rm -f -r ${args_map[targetProject]}:${dataset_prefix}${dataset}
            bq --location=$location mk --dataset --description="Dataset for snapshot" ${args_map[targetProject]}:${dataset_prefix}${dataset}
        else
            ERROR "${msg}"
        fi
    fi
}

function createtableSnapshot() {
    for dataset in $dataset_source_list; do 
        local datasetotal_table_count=0
        #local dataset_table_count=0
        local listtbl=$(bq ls --max_results ${args_map[maxTablesPerDataset]} ${args_map[sourceProject]}:${dataset} | grep 'TABLE' | awk '{print $1}')
        local count_tbl_before=$(bq ls --max_results ${args_map[maxTablesPerDataset]} ${args_map[targetProject]}:${dataset_prefix}${dataset} | grep 'SNAPSHOT' | wc -l)
        local source_dataset_total_table=0
        if [ -z "$listtbl" ]
        then
            source_dataset_total_table=0
        else
            source_dataset_total_table=`echo "$listtbl" | wc -l`
        fi
        source_total_table=$(($source_total_table + $source_dataset_total_table))
        for tbl in $listtbl; do
            checkTableExclude $dataset $tbl
            if [[ $is_skip_table == "false" ]]; then
                (createSnapshot $dataset $tbl)
            else
                WARNING "Skip ${args_map[sourceProject]}:${dataset}.$tbl"
            fi
        done
        wait
        local count_tbl_after=$(bq ls --max_results ${args_map[maxTablesPerDataset]} ${args_map[targetProject]}:${dataset_prefix}${dataset} | grep 'SNAPSHOT' | wc -l)
        dataset_table_count=$(($count_tbl_after - $count_tbl_before))
        if [[ $dataset_table_count == 0 ]]; then
            WARNING "No table has been processed for ${args_map[sourceProject]}:${dataset}"
        else
            INFO "${dataset_table_count} / ${source_dataset_total_table} tables has been processed successfully for ${args_map[sourceProject]}:${dataset}"
            total_snapshot_count=$(($total_snapshot_count + $dataset_table_count))
        fi
    done
}

function createSnapshot() {
    local dataset=$1
    local tbl=$2
    INFO "Start snapshot for ${args_map[sourceProject]}:${dataset}.${tbl}"
    createsnapshottbl $dataset $tbl
    if [[ $msg == *"successfully snapshotted"* ]] | [[ $msg == *"Created"*  ]]; then
        INFO "${msg}"
    elif [[ $msg == *"immutable"* || $msg == *"already exists"* ]]; then
        if [[ ${args_map[overrideSnapshot]} == 'true' ]]; then 
            INFO " Start remove old snapshot table ${args_map[targetProject]}:${dataset_prefix}${dataset}.${tbl}"
            bq rm --f ${args_map[targetProject]}:${dataset_prefix}${dataset}.${tbl}            
            createsnapshottbl $dataset $tbl            
            if [[ $msg == *"successfully snapshotted"* ]] | [[ $msg == *"Created"*  ]]; then
                INFO "${msg}"
            else
                ERROR "${msg}"
            fi
        fi
    else
        ERROR "${msg}"
    fi
}

createsnapshottbl() {
    local dataset=$1
    local tbl=$2
    unset msg
    msg=$(bq query --use_legacy_sql=false "CREATE SNAPSHOT TABLE \`${args_map[targetProject]}.${dataset_prefix}${dataset}.${tbl}\` CLONE \`${args_map[sourceProject]}.${dataset}.${tbl}\`")
}

excludeDataset() {
    local SAVEIFS=$IFS
    local IFS=','
    for dataset in ${args_map[excludeDatasets]}; do 
        dataset_source_list=("${dataset_source_list[@]/$dataset/}")
        INFO "Excluded Dataset:- ${args_map[sourceProject]}:$dataset"
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
        exclude_datset_tbl_ary+=("$dataset_tbl")
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
    INFO "Start creating snapshot for all the datasets."
    createSnapshotDataset
    createtableSnapshot
else
    SCENARIO='DATASET'
    INFO "Start creating snapshot for specific datasets."
    includeDatasets
    createSnapshotDataset
    createtableSnapshot
    if [ -n "${args_map[tables]}" ]; then
        SCENARIO='TABLES'
        INFO "Start creating snapshot for specific tables."
        unset dataset_source_list
        includeTables
        createSnapshotDataset
        for datasettbl in ${include_datset_tbl_ary[@]}; do (
            dataset=${datasettbl//'.'*/} 
            tbl=${datasettbl//*'.'/}
            createSnapshot ${dataset} ${tbl}) 
        done
        wait
    fi
fi
#total_snapshot=$(gcloud logging read 'resource.type=cloud_run_job textPayload=~"successfully snapshotted to*"' | grep 'textPayload' | wc -l )
INFO "Total ${total_snapshot_count} / ${source_total_table} tables successfully snapshotted for ${args_map[sourceProject]}."
INFO "Job completed successfully ... "
######################################################################################### END OF EXECUTION ###########################################################################
