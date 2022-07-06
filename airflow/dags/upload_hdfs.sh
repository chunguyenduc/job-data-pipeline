file_path=$1
file_name=$2

hdfs dfs -test -e /job
not_exist=$?
echo $not_exist

if [[ $not_exist eq 1 ]]
then
    hdfs dfs -mkdir /job
fi

hdfs dfs -copyFromLocal $file_path /job/$file_name