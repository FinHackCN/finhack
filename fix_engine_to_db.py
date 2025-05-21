import os
import re
import sys

def fix_file(file_path):
    print(f"处理文件: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 计数器
    replacements = 0
    
    # 1. 首先将DB.safe_to_sql(df, table, engine, ...)参数修正
    pattern1 = r'(DB\.safe_to_sql\s*\(\s*[^,]+\s*,\s*[^,]+\s*,\s*)engine(\s*,)'
    replacement1 = r'\1db\2'
    content, count1 = re.subn(pattern1, replacement1, content)
    replacements += count1
    
    # 2. 修正DB.to_sql(df, table, engine, ...)参数
    pattern2 = r'(DB\.to_sql\s*\(\s*[^,]+\s*,\s*[^,]+\s*,\s*)engine(\s*,)'
    replacement2 = r'\1db\2'
    content, count2 = re.subn(pattern2, replacement2, content)
    replacements += count2
    
    # 3. 查找所有engine=DB.get_db_engine(db)这样的行并添加注释
    pattern3 = r'^(\s*)engine\s*=\s*DB\.get_db_engine\s*\(\s*db\s*\)(.*)$'
    replacement3 = r'\1# 不需要获取engine对象，直接使用db连接名\n\1# engine = DB.get_db_engine(db)\2'
    content, count3 = re.subn(pattern3, replacement3, content, flags=re.MULTILINE)
    replacements += count3
    
    # 如果有替换，写回文件
    if replacements > 0:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"修复文件 {file_path}，共修改 {replacements} 处")
    
    return replacements

def main():
    # 使用硬编码的绝对路径，确保找到正确的目录
    collector_dir = "/Users/woldy/Code/finhack-dev/finhack/finhack/collector"
    
    if not os.path.exists(collector_dir):
        print(f"找不到collector目录: {collector_dir}")
        return
    
    # 查找所有Python文件
    py_files = []
    for root, dirs, files in os.walk(collector_dir):
        for file in files:
            if file.endswith(".py"):
                py_files.append(os.path.join(root, file))
    
    print(f"找到 {len(py_files)} 个Python文件")
    
    # 处理这些文件
    total_replacements = 0
    for file_path in py_files:
        try:
            total_replacements += fix_file(file_path)
        except Exception as e:
            print(f"处理文件 {file_path} 时出错: {str(e)}")
    
    print(f"总共修复 {total_replacements} 处错误")

if __name__ == "__main__":
    main() 