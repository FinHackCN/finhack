rsync -av --include='*/' --include='factorlist/' --include='factorlist/**'  \
	--include='*.py' --include='*.conf' --exclude='*' --exclude='__pycache__/' \
	./examples/demo-project/ ./finhack/widgets/templates/empty_project 
	
rm -rf ./finhack/widgets/templates/empty_project/data/factors/date_factors/*

sed -i '/password=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/db.conf
sed -i '/token=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/ts.conf
sed -i '/feishu_webhook=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/alert.conf
sed -i '/dingtalk_webhook_webhook=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/alert.conf

python setup.py sdist 
pip install .
ls /root/anaconda3/envs/finhack/lib/python3.9/site-packages
