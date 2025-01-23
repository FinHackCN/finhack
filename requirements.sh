pip install pipreqs
pip install pip-tools

pipreqs . --savepath new_requirements.txt
cat *requirements.txt | awk -F '=' {'print $1'} | sort |uniq >requirements.in
rm -f new_requirements.txt
pip-compile requirements.in --output-file requirements.txt

