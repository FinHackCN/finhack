ps -ef | grep task | awk {'print $2'} | xargs kill
