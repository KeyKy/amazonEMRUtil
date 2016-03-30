#!/bin/bash
#
# Description
# This script wraps AWS EMR to create a spark cluster and run the job then close.
# You should upload your python spark script to S3 and set [SCRIPT_PATH] [SCRIPT_ARGS] before runing this script.
# You can also modify some tuning params to make your jobs run fast.
#
# Created by Mr. Yang, Modified by Baichuan.
#
# TODO:
# Some tuning params were not sure, need some study.
#
# References:
# Doc http://docs.aws.amazon.com/ElasticMapReduce/latest/ReleaseGuide/emr-spark.html
# Beside Doc http://danosipov.com/?p=816
# Tune Apache Spark http://blog.cloudera.com/blog/2015/03/how-to-tune-your-apache-spark-jobs-part-2/
#
# Dependences:
# need spark-config.json at the same folder of this script
#
# Usesage:
# ./start-spark-job.sh
#

LOG_DATE=`date -d "1 day ago" +%Y-%m-%d` #以指定格式设置一天前的年份月份日期
#LOG_DATE=$1
TMP_FILE=tmp.txt
aws s3 ls s3://logarchive.ym/hive/gztables/default_log/dt=${LOG_DATE}/sv=3/ac=304/ > ${TMP_FILE} #将s3这个位置的文件清单输出到tmp.txt文件中
line=`sed -n '$=' ${TMP_FILE}` #$是文件尾，=是打印行号，也就是打印tmp.txt最后一行的行号
if [ -s ${TMP_FILE} ]; then #if[-s file] 文件大小非0时候为真
	echo "Begin processing log for ${LOG_DATE}"
else
	echo "No log for ${LOG_DATE}"
	exit
fi

#change it to your own email!
EMAIL="kangyang@youmi.net" #邮箱的地址

#put py files to s3
APP_NAME=$1
MAIN_PY_FILE="./"${APP_NAME}".py"  #自己python的代码脚本存放路径
SCRIPT_S3_PATH="s3://datamining.ym/dmuser/ykang/com.um.kangyang.spark."${APP_NAME}"/"${APP_NAME}".py" #放置于S3上的路径
aws s3 cp $MAIN_PY_FILE $SCRIPT_S3_PATH #拷贝到s3上

CLUSTER_NAME="dm-ykang-"${APP_NAME}     #设置集群的名字
EMR_LOG="s3://logarchive.ym/emr-logs/" #设置EMRLOG的路径
# subnet-21edf043 is common use
# subnet-f5544297 live in AD Gateway cluster
SUBNET_ID="subnet-21edf043" #subnet id是什么鬼

MEMORY=$2
MASTER_INSTANCE_NUM=1  #主机master的实例数目为1
MASTER_INSTANCE_TYPE="m1.large" #设置机型
DRIVER_MEMORY=${MEMORY} #设置内存
CORE_INSTANCE_NUM=$(($3)) #设置CORE_INSTANCE数目
CORE_INSTANCE_TYPE="m1.large"
TASK_INSTANCE_NUM=$(($4)) #设置TASK_INSTANCE数目
TASK_INSTANCE_TYPE="m1.large"
EXECUTOR_MEMORY=${MEMORY}

# m1.large has 840GB storage 2 Cores 7.5GB Mem
# m3.xlarge has 80G instance storage 4 Cores 15GB Mem

# simple tuning, TODO
TOTAL_CORE_TASK=$(( $CORE_INSTANCE_NUM + $TASK_INSTANCE_NUM )) #task和core的数目相加
EXECUTOR_CORE_COUNT=$(( $TOTAL_CORE_TASK * $(($5)) )) #所有执行者core的数目

echo "core_task_total_instance_number: "$EXECUTOR_CORE_COUNT

SCRIPT_ARGS1="s3://logarchive.ym/hive/gztables/default_log/dt=${LOG_DATE}/sv=3/ac=304/*"
SCRIPT_ARGS2="s3://datamining.ym/user_profile/last5/${LOG_DATE}"

#create cluster #根据 EMR_LOG master core task节点的个数 创建emr集群
CREATE_RESULT=`aws emr create-cluster --name "$CLUSTER_NAME"\
 --release-label emr-4.1.0 \
 --no-termination-protected \
 --log-uri $EMR_LOG \
 --applications Name=Spark \
 --ec2-attributes KeyName=cn-ec2-test-key,SubnetId=$SUBNET_ID \
 --instance-groups InstanceCount=$MASTER_INSTANCE_NUM,Name=Master,InstanceGroupType=MASTER,InstanceType=$MASTER_INSTANCE_TYPE InstanceCount=$CORE_INSTANCE_NUM,Name=Core,InstanceGroupType=CORE,InstanceType=$CORE_INSTANCE_TYPE InstanceCount=$TASK_INSTANCE_NUM,Name=Task,InstanceGroupType=TASK,InstanceType=$TASK_INSTANCE_TYPE \
 --configuration file://spark-config.json \
 --use-default-roles`

# get cluster id 获取集群的ID号
CLUSTER_ID=`echo $CREATE_RESULT | grep -Po '(?<="ClusterId": ")[^"]*'` #
if [ -z $CLUSTER_ID ]; then
    echo "error:cluster_id empty"
    exit
fi
echo "cluster_id:" $CLUSTER_ID

# check create cluster done 
TIMEOUT=1
while :
do
    TOTAL_INSTANCE="$(($MASTER_INSTANCE_NUM+$CORE_INSTANCE_NUM+$TASK_INSTANCE_NUM))"
    CURRENT_INSTANCE=`aws emr list-instances --cluster-id $CLUSTER_ID | grep -o RUNNING | wc -l` #通过该命令判断集群的状态
    echo "total_instance:" $TOTAL_INSTANCE "current_instance:" $CURRENT_INSTANCE
    if [ $TOTAL_INSTANCE -eq $CURRENT_INSTANCE ]; then
        break
    fi
    if [ $TIMEOUT -gt 30 ]; then #如果大于30则创建超时
        echo "create cluster timeout"
        exit
    fi
    TIMEOUT=$(($TIMEOUT+1))
    sleep 60
done
echo "Cluster ready"

#run your task
STEP_RESULT=`aws emr add-steps --cluster-id $CLUSTER_ID --steps Type=Spark,Name=$CLUSTER_NAME,ActionOnFailure=TERMINATE_CLUSTER,Args=[--deploy-mode,cluster,--master,yarn-cluster,--driver-memory,$DRIVER_MEMORY,--executor-memory,$EXECUTOR_MEMORY,--total-executor-cores,$EXECUTOR_CORE_COUNT,$SCRIPT_S3_PATH]` #run多个文件呢？
STEP_ID=`echo $STEP_RESULT | grep -Po 's-[^"]*'`
echo "step_id:" $STEP_ID

#check task status
TIMEOUT=1
while :
do
    STEP_STATUS=`aws emr list-steps --cluster-id $CLUSTER_ID --step-ids $STEP_ID | grep -Po '(?<="State": ")[^"]*'`
    if [ $STEP_STATUS = "COMPLETED" ] || [ $STEP_STATUS = "FAILED" ];then #获取任务状态
        echo "job status $STEP_STATUS, terminate clusters"
	if [ $STEP_STATUS = "FAILED" ];then
		echo "Failed" | mail -s "Log analysis failed.." $EMAIL #安装mailutils工具后可以使用mail
	fi
        aws emr terminate-clusters --cluster-ids $CLUSTER_ID
        exit
    fi
    if [ $TIMEOUT -gt 480 ]; then
        echo "job running timeout"
        echo "Timeout" | mail -s "Log analysis Timeout.." $EMAIL
        aws emr terminate-clusters --cluster-ids $CLUSTER_ID
        exit
    fi
    echo "job running:" $TIMEOUT "minute"
    TIMEOUT=$(($TIMEOUT+2))
    sleep 120
done
