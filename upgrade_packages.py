import os
import re
from pathlib import Path
import shutil

# 定义需要替换的内容
REPLACEMENTS = {
    # 1. configparser 模块的替换
    r"SafeConfigParser": "ConfigParser",  # SafeConfigParser 被移除
    r"readfp\(": "read_file(",  # readfp 被 read_file 替换

    # 2. collections 模块的替换
    r"collections\.MutableMapping": "collections.abc.MutableMapping",  # MutableMapping 移动到 collections.abc
    r"collections\.Mapping": "collections.abc.Mapping",  # Mapping 移动到 collections.abc
    r"collections\.Sequence": "collections.abc.Sequence",  # Sequence 移动到 collections.abc

    # 3. asyncio 模块的替换
    r"@asyncio\.coroutine": "async def",  # @asyncio.coroutine 被 async def 替换
    r"yield from": "await",  # yield from 被 await 替换

    # 4. typing 模块的替换
    r"typing\.List": "list",  # typing.List 被 list 替换
    r"typing\.Dict": "dict",  # typing.Dict 被 dict 替换
    r"typing\.Tuple": "tuple",  # typing.Tuple 被 tuple 替换
    r"typing\.Set": "set",  # typing.Set 被 set 替换
    r"typing\.Optional": "| None",  # typing.Optional[T] 被 T | None 替换
    r"typing\.Union\[([^]]+)\]": r"\1",  # typing.Union[A, B] 被 A | B 替换

    # 5. 其他常见的替换
    r"os\.path\.join\(": "pathlib.Path('/').joinpath(",  # 推荐使用 pathlib
    r"os\.path\.exists\(": "pathlib.Path().exists(",  # 推荐使用 pathlib
    r"os\.path\.isfile\(": "pathlib.Path().is_file(",  # 推荐使用 pathlib
    r"os\.path\.isdir\(": "pathlib.Path().is_dir(",  # 推荐使用 pathlib

    # 6. 字符串格式化替换
    r"\.format\(": "f\"",  # str.format() 被 f-string 替换
    r"%s": "{}",  # %s 被 {} 替换
    r"%d": "{}",  # %d 被 {} 替换

    # 7. 其他 Python 3.13 的变更
    r"threading\._allocate_lock": "threading.Lock",  # _allocate_lock 被 Lock 替换
    r"subprocess\.getoutput\(": "subprocess.run(..., capture_output=True, text=True).stdout",  # getoutput 被 run 替换
}

def upgrade_file(file_path, backup=True):
    """升级单个文件"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        # 对文件内容进行替换
        updated = False
        for pattern, replacement in REPLACEMENTS.items():
            if re.search(pattern, content):
                content = re.sub(pattern, replacement, content)
                updated = True

        # 如果文件内容有更新，则写入文件
        if updated:
            if backup:
                # 备份原始文件
                backup_path = str(file_path) + ".bak"
                shutil.copyfile(file_path, backup_path)
                print(f"Backup created: {backup_path}")

            with open(file_path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"Upgraded: {file_path}")
        else:
            print(f"No changes needed: {file_path}")

    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def upgrade_directory(directory, backup=True):
    """升级目标目录下的所有 Python 文件"""
    directory = Path(directory)
    if not directory.is_dir():
        print(f"Error: {directory} is not a valid directory.")
        return

    # 遍历目录下的所有 Python 文件
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = Path(root) / file
                upgrade_file(file_path, backup=backup)

if __name__ == "__main__":
    import argparse

    # 解析命令行参数
    parser = argparse.ArgumentParser(description="Upgrade Python files to be compatible with Python 3.13.")
    parser.add_argument("--directory", type=str, help="The target directory to upgrade.")
    parser.add_argument("--no-backup", action="store_true", help="Disable backup of original files.")
    args = parser.parse_args()

    # 目标目录
    target_directory = args.directory if  args.directory  else "./upgrade_packages"

    # 检查目标目录是否存在
    if not os.path.exists(target_directory):
        print(f"Error: Directory {target_directory} does not exist.")
    else:
        print(f"Starting upgrade for directory: {target_directory}")
        upgrade_directory(target_directory, backup=not args.no_backup)
        print("Upgrade completed.")
