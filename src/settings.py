# -*- encoding: utf-8 -*-
''' alarm_stock.py 的参数 '''

import logging.handlers
import os


DEBUG = False

# 数据目录
dir_data = '../datas'
# 声音文件
f_name_audio = os.path.join(dir_data, 'Ron Korb-Todaiji.mp3')
# SQLite3文件
f_name_database = os.path.join(dir_data, 'alarm_stock.db')
sql_url = f'sqlite:///{f_name_database}'
# 工作日志
if DEBUG:
    log_level = logging.DEBUG
else:
    log_level = logging.INFO

f_name_log = 'alarm_stock.log'
# log_format = logging.Formatter(
log_format = (
        '[%(asctime)s - %(process)d:%(thread)d_%(threadName)s - %(levelname)s'
        ' - %(name)s:%(lineno)d] - %(message)s'
        )
# th = logging.handlers.TimedRotatingFileHandler(
#         filename=os.path.join(dir_data, f_name_log),
#         when='D',
#         interval=7,
#         backupCount=7,
#         encoding='utf-8',
#         )
# th.setLevel(log_level)
# th.setFormatter(log_format)
logging.basicConfig(
        filename=os.path.join(dir_data, f_name_log),
        format=log_format,
        level=log_level
        )

# 开市休眠20秒
n_sleep = 20
# 闭市后，监控程序继续运行的时间
n_continue_run = 30
# 插件目录名
dir_plugin = 'plugins'
