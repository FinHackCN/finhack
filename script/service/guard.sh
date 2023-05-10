# #! 不是注释符，而是指定脚本由哪个解释器来执行，
# #! 后面有一个空格，空格后面为解释器的全路径且必须正确。
#! /bin/sh

while true ; do

# ps aux --> a 为显示其他用户启动的进程；
#            u 为显示启动进程的用户名与时间；
#            x 为显示系统属于自己的进程；
# grep -v 指定文本 --> 输出不包含指定文本的那一行文本信息
# wc -l --> 输出文件中的行数(-l --> 输出换行符统计数)
# 整数比较：-lt -> 小于，-le -> 小于等于，-gt -> 大于，-ge -> 大于等于，-eq ->等于，-ne -> 不等于
# 0 -> 标准输入，1 -> 标准输出，2 - > 标准错误信息输出
# /dev/null --> Linux的特殊文件，它就像无底洞，所有重定向到它的信息数据都会消失！
# 2 > /dev/null --> 重定向 stderr 到 /dev/null，1 >& 2 --> 重定向 stdout 到 stderr
    PRO_NOW=$(ps aux|grep "command/cmd_autotrain.py 10 40" | grep -v grep | wc -l)
    if [ $PRO_NOW -lt 2 ]; then
        cd /home/woldy/finhack
        nohup python -u command/cmd_autotrain.py >>data/logs/autotrain.log 10 40 &
    fi
    
    sleep 60   
    
    
done
# exit 用来结束脚本并返回状态值，0 - 为成功，非零值为错误码，取值范围为0 ~ 255。
exit 0
