ps -ef | grep "npm run dev" | awk {'print $2'} | xargs kill
ps -ef | grep "npm run serve" | awk {'print $2'} | xargs kill
ps -ef | grep "vue-cli-service" | awk {'print $2'} | xargs kill


ps -ef | grep "manage.py runserver 0.0.0.0:8801" | awk {'print $2'} | xargs kill
cd http/web
nohup npm run dev --finhack >> ../../data/logs/ui_frontend.log &
cd ../backend/
nohup python3 manage.py runserver 0.0.0.0:8801 >> ../../data/logs/ui_backend.log &


#docsify serve 
