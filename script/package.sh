#!/bin/bash
# 尝试执行mysqldump，但不立即重定向输出
DUMP_OUTPUT=$(mysqldump -uroot -p --no-data finhack 2>&1)
cp requirements.txt finhack
# 检查命令的退出状态
if [ $? -eq 0 ]; then
  # mysqldump成功，将输出写入文件
  echo "$DUMP_OUTPUT" > ./database/finhack_structure.sql
  echo "Database structure dumped successfully."
else
  # mysqldump失败，可能是由于密码错误
  echo "Failed to dump database structure. Error was:"
  echo "$DUMP_OUTPUT"
fi

rm -rf dist/*
rm -rf build/*
rm -rf finhack.egg-info

rsync -av --include='*/' --include='factorlist/' --include='factorlist/**'  \
	--include='*.py' --include='*.conf' --include='index.html' --exclude='*' --exclude='__pycache__/' \
	./examples/demo-project/ ./finhack/widgets/templates/empty_project 
	
rm -rf ./finhack/widgets/templates/empty_project/data/factors/date_factors/*

rm -f ./finhack/widgets/templates/empty_project/data/config/factorlist/alphalist/alpha999

sed -i '/password=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/db.conf
sed -i '/token=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/ts.conf
sed -i '/feishu_webhook=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/alert.conf
sed -i '/dingtalk_webhook_webhook=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/alert.conf
sed -i '/api_key=/s/=.*/=/' ./finhack/widgets/templates/empty_project/data/config/ai.conf

DIRECTORY="./finhack/widgets/templates/empty_project/strategy"

# 白名单文件名数组
WHITELIST=(
    "__init__.py"
    "AITopNStrategy.py"
    "ChatGPTStrategy.py"
    "DemoStrategy.py"
)

# 将白名单数组转换为grep的排除模式
PATTERN=$(printf "|%s" "${WHITELIST[@]}")
PATTERN="${PATTERN:1}"

# 查找目录下的所有.py文件，并检查是否在白名单中
find "$DIRECTORY" -type f -name "*.py" | grep -vE "($PATTERN)" | xargs rm -f


python setup.py sdist 
pip install .


# 设置dist目录的路径
DIST_PATH="./dist"

# 遍历dist目录中的所有文件
for FILE in "$DIST_PATH"/*.tar.gz; do
  if [[ "$FILE" =~ ([^/]+)-([0-9]+\.[0-9]+\.[0-9]+(\.dev[0-9]+)?)\.tar\.gz$ ]]; then
    PACKAGE_NAME="${BASH_REMATCH[1]}"
    PACKAGE_VERSION="${BASH_REMATCH[2]}"

    # 使用PyPI API检查包是否已经存在
    RESPONSE_CODE=$(curl -s -o /dev/null -w "%{http_code}" "https://pypi.org/pypi/${PACKAGE_NAME}/${PACKAGE_VERSION}/json")

    if [ "$RESPONSE_CODE" -eq 200 ]; then
      echo "Package ${PACKAGE_NAME} version ${PACKAGE_VERSION} already exists on PyPI. Skipping upload for this version."
    else
      echo "Package ${PACKAGE_NAME} version ${PACKAGE_VERSION} does not exist on PyPI. Uploading..."
      twine upload "$FILE"
    fi
  else
    echo "Skipping $FILE, does not appear to be a valid package file."
  fi
done


ls /root/anaconda3/envs/finhack/lib/python3.13/site-packages
