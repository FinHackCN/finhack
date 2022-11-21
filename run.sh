ps -ef | grep task | awk {'print $2'} | xargs kill
python command/cmd_task.py
