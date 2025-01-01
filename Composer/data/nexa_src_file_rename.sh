#! /bin/bash
#########################################################################################################################################
#                                                                                                                                       #
#                       SCRIPT NAME      : optum_src_file_rename.sh                                                                     #
#                       VERSION          : v1                                                                                           #
#                       AUTHOR           : IRSHAD KHAN <IRSHAD.KHAN@DAVITA.COM>                                                         #
#                       DATE OF CREATION : FEB 10 2022                                                                                  # 
#                       PURPOSE          : RENAMES THE SOURCE FILES FOR PROCESSING                                                      #
#                                                                                                                                       #
#########################################################################################################################################

cd /home/airflow/gcs/data
cat 'list_files.txt' | while read LINE; do
    echo $LINE
    for VARIABLE in $LINE;do
    
    eval "cl_var=$(echo $VARIABLE  | sed 's/ //g')"
    eval "folder_nm=$(echo $cl_var | sed 's:/[^/]*$::')"
    eval "file_nm=$(echo $cl_var | sed 's:.*/::')"
    eval "prefix=$(echo $file_nm | grep -o -E '[0-9]+'  | sed ':a;N;$!ba;s/\n//g')"
    eval "upd_file_nm=$(echo $file_nm | sed 's/[0-9]*//g' | sed 's/_//g')"
    eval "new_uri=$(echo $folder_nm'/'$prefix'_'$upd_file_nm)"

    if [ $cl_var == $new_uri ]
	then 
	   echo "file_name are Same - skipping to next file"
    else 
	   echo $(gsutil -m mv -r $cl_var $new_uri )
	
	fi
	
    done

done

rm /home/airflow/gcs/data/list_files.txt