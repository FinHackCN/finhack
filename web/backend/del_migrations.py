# -*- coding: utf-8 -*-

import os

exclude = ["venv"] # 需要排除的文件目录
for root, dirs, files in os.walk('.'):
    dirs[:] = [d for d in set(dirs) - set(exclude)]
    if 'migrations' in dirs:
        dir = dirs[dirs.index('migrations')]
        for root_j, dirs_j, files_j in os.walk(os.path.join(root, dir)):
            for file_k in files_j:
                if file_k != '__init__.py':
                    dst_file = os.path.join(root_j, file_k)
                    print('删除文件>>> ', dst_file)
                    os.remove(dst_file)