#!/bin/bash
#遍历文件夹及其子文件夹内所有文件，并查看各个文件行数、大小
# $1 传入目标文件夹路径
dst_dir=$1

#子函数getdir
# $1 接受函数传入的参数
function getdir()
{   
    for element in `ls $1`
    do  
        file=$1"/"$element
        if [ -d $file ]
        then 
            getdir $file
        else
            echo $file >> /tmp/result.out #将结果保存到/root/result.out
        fi  
    done
}

getdir $dir #引用子函数
for line in `cat /tmp/result.out`  #读取文件 result.out的每行
do
    filecount=`wc -l $line | awk '{print $1}'`  #读取文件行数
    echo  $line   $filecount  >> /tmp/dir.out  #将结果文件写入到dir.out中
done
