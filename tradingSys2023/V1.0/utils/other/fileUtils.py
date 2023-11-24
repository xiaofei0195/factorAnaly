# -*- coding:utf-8 -*-
import sys
import os
# 把当前文件所在文件夹的父文件夹路径加入到PYTHONPATH
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime

def write(log_file,msg):
    dt = datetime.today()
    now = dt.strftime("%Y-%m-%d %H:%M:%S")
    line = '%s:\n%s\n' % (now, msg)
    log_file.write(line)
