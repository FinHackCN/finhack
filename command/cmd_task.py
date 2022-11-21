import sys
import os
import time

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from factors.taskRunner import taskRunner

taskRunner.runTask('all')