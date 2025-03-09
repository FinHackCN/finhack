pip install pipreqs
pip install pip-tools

pipreqs . --savepath new_requirements.txt
sed -i -E 's/^alphalens([>=<])/alphalens-reloaded\1/g' new_requirements.txt  # 方法一
sed -i -E 's/^empyrical([>=<])/empyrical-reloaded\1/g' new_requirements.txt
sed -i -E 's/^grpc([>=<])/grpcio\1/g' new_requirements.txt
sed -i -E 's/^codegen([>=<])/pandas\1/g' new_requirements.txt
cat new_requirements.txt | awk -F '=' {'print $1'} | sort |uniq >requirements.in
echo "mysql-connector" >> requirements.in
echo "tushare" >> requirements.in
echo "pyarrow"  >> requirements.in
echo "TA-Lib"  >> requirements.in
echo "mysql-connector-python"  >> requirements.in
pip-compile requirements.in --output-file requirements.txt
rm -f new_requirements.txt
sed -i '/^#/d; /^$/d; /^--/d' requirements.txt
