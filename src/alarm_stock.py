# -*- encoding: utf-8 -*-
''' 股票监控
目前的限制条件：
    报警程序的执行时间 < 1分钟

定时启动:
    更新k线数据 ---> 执行报警程序

报警程序:（单个线程）
    遍历n个报警程序:
        遍历n个股票:
            遍历n个k线周期:
                执行单个报警算法 (以插件形式存在)
'''

import datetime
import dateutil
import importlib
import jqdatasdk
import json
import os
import pandas as pd
import pdb
import re
import schedule
import sqlalchemy
import threading
import time
import vlc

from abc import ABC, abstractmethod
from pandarallel import pandarallel
from pandas.tseries.offsets import Second, Minute, Hour, Day
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

# our apps
import settings as settings

__version__ = 1.3

# 日志
logger = settings.logging.getLogger(__name__)
# logger.addHandler(settings.th)
# 工作进程数量
pandarallel.initialize(nb_workers=2)

def to_csv_mt5(df, f_name=None, write_mode='w'):
    ''' DataFrame写入mt5格式的csv
        info        DataFrame
        f_out       csv文件名
        write_mode   文件写入模式
    df格式:
        >>> df_today[:2]
                                  open       high        low      close
        date
        2020-10-14 09:31:00  4830.4575  4830.4575  4819.1404  4819.3108
        2020-10-14 09:32:00  4816.8566  4819.1151  4816.2843  4819.1151       
    f_name格式:
        列:     '<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>',
                '<TICKVOL>', '<VOL>', '<SPREAD>'
        分割符: \t
    '''
    if f_name is None:
        f_name = os.path.join(settings.dir_data, 'tmp.csv')
    arr_header = ['<DATE>', '<TIME>', '<OPEN>', '<HIGH>', '<LOW>', '<CLOSE>']
    arr_name = ['s_date', 's_time', 'open', 'high', 'low', 'close']
    info = pd.DataFrame(df, columns=arr_name)
    info.loc[:, 's_date'] = info.parallel_apply(
            lambda o: o.name.strftime('%Y.%m.%d'), axis=1,
            )
    info.loc[:, 's_time'] = info.parallel_apply(
            lambda o: o.name.strftime('%H:%M:%S'), axis=1,
            )
    if write_mode == 'w':
        header = arr_header
    else:
        header = False
    info.to_csv(
            f_name, float_format='%.2f', mode=write_mode, index=False,
            header=header, columns=arr_name,
            )


class TimingStart:
    ''' 定时开始任务
        更新k线数据 ---> 遍历报警程序(所有k线周期)
    '''
    # 闭市后，监控程序继续运行的时间
    t_continue_run = datetime.timedelta(seconds=settings.n_continue_run)
    # k线数据对象实例 (用在所有的报警算法中)
    obj_KlineInfo = None
    # vlc
    obj_sound = None
    # 仅第一次运行
    only_once = None

    def __init__(self):
        self.only_once = True
        self.obj_KlineInfo = KlineInfo()
        self.init_audio()

    def __del__(self):
        ''' 析构函数 '''
        self.obj_sound.stop()

    def event_timer(self):
        ''' 定时器事件
        开市时间：每20秒启动一次时间监控程序。
        闭市时间：每小时启动一次时间监控程序。
        在指定时间（HH:MM:03），运行报警检测程序。
        '''
        schedule.every().minute.at(':03').do(self.run_threaded, self.job)
        while True:
            if self.only_once:
                # 第0次运行
                market_opening, sleep_seconds = True, settings.n_sleep
            else:
                market_opening, sleep_seconds = self.calc_sleep_seconds()
            logger.debug(f'event_timer() ... market_opening: {market_opening}, sleep_seconds: {sleep_seconds}s')
            if market_opening or settings.DEBUG:
                # 开市时间，运行报警检测程序
                schedule.run_pending()
            time.sleep(sleep_seconds)

    def run_threaded(self, job_func):
        ''' 报警检测程序，在子线程中并发执行 '''
        job_thread = threading.Thread(target=job_func)
        job_thread.start()

    def job(self):
        ''' 定时执行的任务 '''
        obj = self.obj_KlineInfo
        try:
            flag = obj.run_cron(self)
            if flag and not self.only_once:
                self.play_audio()
        except ValueError as e:
            logger.error(f'{e}')
        else:
            if self.only_once:
                self.only_once = False

    def calc_sleep_seconds(self):
        ''' 计算休眠时间 '''
        market_opening = False
        now = datetime.datetime.now()
        now_weekday = now.date().weekday()
        t_am_begin = datetime.datetime(now.year, now.month, now.day, 9, 30)
        t_am_end = datetime.datetime(now.year, now.month, now.day, 11, 30)
        t_pm_begin = datetime.datetime(now.year, now.month, now.day, 13)
        t_pm_end = datetime.datetime(now.year, now.month, now.day, 15, 0)
        if now_weekday < 5:
            if now < t_am_begin:
                # 早市之前
                sleep_seconds = (t_am_begin - now).total_seconds()
            elif t_am_begin <= now <= t_am_end + self.t_continue_run:
                # 早上开市期间
                sleep_seconds = settings.n_sleep
                market_opening = True
            elif t_am_end < now < t_pm_begin:
                # 中午休市时间
                sleep_seconds = (t_pm_begin - now).total_seconds()
            elif t_pm_begin <= now <= t_pm_end + self.t_continue_run:
                # 下午开市期间
                sleep_seconds = settings.n_sleep
                market_opening = True
            else:
                # 下午闭市之后
                if now_weekday == 4:
                    # 周五
                    wait_day = datetime.timedelta(days=3)
                else:
                    # 周一..周四
                    wait_day = datetime.timedelta(days=1)
                sleep_seconds = (t_am_begin + wait_day - now).total_seconds()
        else:
            # 周末
            wait_day = datetime.timedelta(days=7 - now_weekday)
            sleep_seconds = (t_am_begin + wait_day - now).total_seconds()
        sleep_seconds = int(sleep_seconds)
        if sleep_seconds < 1:
            # 开市之前的(0.9..0]秒
            sleep_seconds = settings.n_sleep
        return (market_opening, int(sleep_seconds))

    def init_audio(self):
        ''' 初始化声音系统
            >>> import vlc
            >>> f_name = 'src/audio.mp3'
            >>> p = vlc.MediaPlayer(f_name)
            >>> p.play()        # 播放
            >>> p.pause()       # 暂停 or 继续
            >>> p.stop()        # 停止
            >>> p.is_playing()  # 查询播放状态
            >>> p.get_state()   # 查询播放状态
        '''
        f_name = settings.f_name_audio
        if not os.path.exists(f_name):
            msg = "声音文件不存在"
            raise ValueError(msg)
        self.obj_sound = vlc.MediaPlayer(f_name)

    def play_audio(self):
        self.obj_sound.stop()
        self.obj_sound.play()


class DataTable:
    ''' 数据表信息 '''
    # 数据库引擎
    engine = None
    # 数据表名称
    db_name = None
    # 数据表
    arr_name_table = (
            'market_info',
            'stock_code_info',
            'stock_type_info',
            'alarm_program_info',
            'alarm_message',
            'QuotesDataSource_account',
             )
    # 删除数据表
    sql_table_drop = '''drop table "{t_name}";'''
    # 清空数据表中的记录
    sql_table_delete = '''delete from "{t_name}";'''
    # 创建数据表: 交易市场
    sql_table_create__market_info = '''
            CREATE TABLE "market_info" (
                    "code_suffix" TEXT NOT NULL,
                    "trading_market" TEXT,
                    "sample_code" TEXT,
                    "stock_name" TEXT,
                    PRIMARY KEY ("code_suffix")
                    );
            '''
    # 创建数据表: 股票代码
    sql_table_create__stock_code_info = '''
            CREATE TABLE "stock_code_info" (
                    "code" TEXT NOT NULL,
                    "display_name" TEXT,
                    "name" TEXT,
                    "start_date" DATE,
                    "end_date" DATE,
                    "type" TEXT,
                    PRIMARY KEY ("code")
                    );
            '''
    # 创建数据表: 股票类型
    sql_table_create__stock_type_info = '''
            CREATE TABLE "stock_type_info" (
                    "type" TEXT NOT NULL,
                    "name" TEXT,
                    PRIMARY KEY ("type")
                    );
            '''
    # 创建数据表: k线数据
    sql_table_create__kline_data = '''
            CREATE TABLE "{t_name}" (
                    "date" DATETIME NOT NULL,
                    "open" FLOAT,
                    "high" FLOAT,
                    "low" FLOAT,
                    "close" FLOAT,
                    PRIMARY KEY ("date")
                    );
            '''
    # 创建数据表: 报警程序
    sql_table_create__alarm_program = '''
            CREATE TABLE "alarm_program_info" (
                    "algorithm" TEXT Not NULL,
                    "arr_stock_code" TEXT NOT NULL,
                    "arr_period" TEXT NOT NULL,
                    "other_kwargs" TEXT,
                    "remark" TEXT
                    );
            '''
    # 创建数据表: 报警信息
    sql_table_create__alarm_message = '''
            CREATE TABLE "alarm_message" (
                    "s_now" TEXT NOT NULL,
                    "stock_code" TEXT NOT NULL,
                    "period" TEXT NOT NULL,
                    "message" TEXT,
                    PRIMARY KEY ("s_now", "stock_code", "period")
                    );
            '''
    # 创建数据表: 数据源账号
    sql_table_create__QuotesDataSource_account = '''
            CREATE TABLE "QuotesDataSource_account" (
                    "name_source" TEXT NOT NULL,
                    "username" TEXT,
                    "password" TEXT,
                    PRIMARY KEY ("name_source")
                    );
            '''

    def __init__(self, db_name=settings.sql_url):
        self.set_database_name(db_name)

    def get_database_name(self):
        ''' 数据库名 '''
        return self.db_name

    def set_database_name(self, db_name=None):
        if db_name is None:
            db_name = settings.f_name_database
        if db_name == self.db_name:
            return
        if self.engine:
            self.engine.dispose()
        self.db_name = db_name
        self.engine = create_engine(self.db_name, echo=False)

    def table_is_exists(self, t_name):
        ''' 检查数据表的存在 '''
        return self.engine.has_table(t_name)

    def sql_execute(self, s_sql, info={}):
        ''' 在数据表中，执行sql语句 '''
        sql = s_sql.format(**info)
        logger.debug(f'sql: {sql}')
        sess = Session(bind=self.engine)
        sess.execute(sql)
        sess.commit()
        sess.close()

    def table_drop(self, t_name):
        ''' 删除数据表 '''
        self.sql_execute(self.sql_table_drop, {'t_name': t_name})

    def table_empty(self, t_name):
        ''' 清空数据表的数据 '''
        self.sql_execute(self.sql_table_delete, {'t_name': t_name})

    def table_create__kline(self, t_name):
        ''' 创建数据表: k线数据 '''
        logger.debug(f'创建数据表 {t_name}')
        self.sql_execute(self.sql_table_create__kline_data, {'t_name': t_name})

    def table_create__market_info(self):
        ''' 创建数据表: 市场信息 '''
        self.sql_execute(self.sql_table_create__market_info)

    def table_create__stock_code_info(self):
        ''' 创建数据表: 股票代码 '''
        self.sql_execute(self.sql_table_create__stock_code_info)

    def table_create__stock_type_info(self):
        ''' 创建数据表: 股票类型 '''
        self.sql_execute(self.sql_table_create__stock_type_info)

    def table_create__alarm_program(self):
        ''' 创建数据表: 报警程序 '''
        self.sql_execute(self.sql_table_create__alarm_program)

    def table_create__alarm_message(self):
        ''' 创建数据表: 报警程序 '''
        self.sql_execute(self.sql_table_create__alarm_message)

    def table_create__QuotesDataSource_account(self):
        ''' 创建数据表: 数据源账号 '''
        self.sql_execute(self.sql_table_create__QuotesDataSource_account)

    def read_db__kline(self, t_name):
        ''' 从数据表读取k线数据 '''
        if self.table_is_exists(t_name):
            df = pd.read_sql(t_name, con=self.engine, index_col='date')
        else:
            raise ValueError(f'{t_name}数据表不存在')
        return df

    def save_db__kline(self, df, t_name):
        ''' k线数据写入数据表 '''
        if not self.table_is_exists(t_name):
            self.table_create__kline(t_name)
        df.to_sql(t_name, con=self.engine, if_exists='append', chunksize=1000)

    def read_db__stock_code(self):
        ''' 读取数据表: 股票代码 '''
        df = pd.read_sql(
                'stock_code_info', con=self.engine, index_col='code',
                columns=['code', 'display_name'],
                )
        return df

    def read_db__alarm_program(self):
        ''' 读取数据表: 报警程序 '''
        t_name = 'alarm_program_info'
        df = pd.read_sql(t_name, con=self.engine)
        return df

    def save_db__alarm_program(self, df):
        ''' 报警程序写入数据表 '''
        t_name = 'alarm_program_info'
        if not self.table_is_exists(t_name):
            self.table_create__alarm_program(t_name)
        df.to_sql(t_name, con=self.engine, if_exists='append', chunksize=1000)

    def read_db__alarm_message(self):
        ''' 读取数据表: 报警信息 '''
        index_name = ['s_now', 'stock_code', 'period']
        df = pd.read_sql('alarm_message', con=self.engine, index_col=index_name)
        return df

    def save_db__alarm_message(self, df):
        ''' 报警信息写入数据表 '''
        t_name = 'alarm_message'
        if not self.table_is_exists(t_name):
            self.table_create__alarm_message(t_name)
        df.to_sql(t_name, con=self.engine, if_exists='append', chunksize=1000)

    def read_db__QuotesDataSource_account(self):
        ''' 读取数据表: 数据源账号 '''
        df = pd.read_sql('QuotesDataSource_account', con=self.engine)
        df.set_index('name_source', inplace=True)
        return df

    def save_db__QuotesDataSource_account(self, df):
        ''' 数据源账号, 写入数据表 '''
        t_name = 'QuotesDataSource_account'
        if not self.table_is_exists(t_name):
            self.table_create__alarm_program(t_name)
        df.to_sql(t_name, con=self.engine, if_exists='append')

    def table__init(self):
        ''' 初始化数据表 '''
        self.table_create__market_info()
        self.table_create__stock_code_info()
        self.table_create__stock_type_info()
        self.table_create__alarm_program()
        self.table_create__alarm_message()
        self.table_create__QuotesDataSource_account()


class KlineInfo:
    ''' k线数据
    从数据库读取需要报警的股票信息
    k线数据, info_stock, dict
            key     stock_code
            value   SingleStockInfo()
    报警程序, info_program, dict
            key     (algorithm, arr_tock_code, arr_program, other_kwargs)
            value   SingleAlarmProgram()
    '''
    # 数据表
    obj_DataTable = None
    # 数据源
    obj_DataSource = None
    # k线数据
    info_stock = None
    # 报警程序
    info_program = None
    # 报警信息的内容
    arr_alarm_msg = None
    # k线分析周期
    period_base = None

    def __init__(self):
        self.period_base = '1m'
        self.info_program = {}
        self.info_stock = {}
        self.obj_DataTable = DataTable()
        self.obj_DataSource = JqData(self.obj_DataTable)
        # 获取报警信息(k线数据，报警程序)
        self.get_alarm_info()

    def run_cron(self, only_once):
        ''' 定时执行 '''
        # 下载最新的行情数据
        logger.debug('下载最新的行情数据 ...')
        self.download_new_data()
        # 遍历报警程序
        logger.debug('遍历报警程序 ...')
        flag = self.traverse_the_alarm_program(only_once)
        return flag

    def download_new_data(self):
        ''' 下载最新行情 '''
        arr_code = list(self.info_stock.keys())
        start_date = min([
                obj.get_last_date(self.period_base)
                for obj in self.info_stock.values()
                ])
        info = self.obj_DataSource.get_data_missing(
                arr_code, self.period_base, start_date, offset_right=True,
                )
        for code, df in info.items():
            obj_stock = self.info_stock[code]
            df_new = obj_stock.data_merge(self.period_base, df)
            if df_new.empty:
                continue
            # 下载数据，写入数据表
            obj_stock.obj_db.save_db__kline(df_new, obj_stock.table_name)
            # 更新k线其它周期的数据
            obj_stock.period_update()

    def get_alarm_info(self):
        ''' 从数据库读取需要报警的股票代码 '''
        df_name = self.obj_DataTable.read_db__stock_code()
        df_alarm_program = self.obj_DataTable.read_db__alarm_program()
        for i, row in df_alarm_program.iterrows():
            logger.debug(f'{i}# \t({row.algorithm}, {row.arr_stock_code}, {row.arr_period}, {row.other_kwargs}), {row.remark}')
            # k线数据
            arr_stock_code = json.loads(row.arr_stock_code)
            arr_period = json.loads(row.arr_period)
            for stock_code in arr_stock_code:
                obj_code = self.info_stock.get(stock_code)
                if obj_code is None:
                    stock_name = df_name.loc[stock_code, 'display_name']
                    obj_code = SingleStockInfo(
                            stock_code, stock_name, self.period_base,
                            obj_db=self.obj_DataTable,
                            obj_source=self.obj_DataSource,
                            )
                    self.info_stock[stock_code] = obj_code
                for period in arr_period:
                    if period != obj_code.period_base:
                        obj_code.period_add(period)
            # 报警程序（若有重复值，取最后一个）
            if row.other_kwargs:
                other_kwargs = json.loads(row.other_kwargs)
            else:
                other_kwargs = None
            info = {
                    'algorithm': row.algorithm,
                    'arr_stock_code': arr_stock_code,
                    'arr_period': arr_period,
                    'other_kwargs': other_kwargs,
                    'remark': row.remark,
                    'data_kline': obj_code.data_kline
                    }
            obj_program = SingleAlarmProgram(info)
            key = (
                    row.algorithm, row.arr_stock_code, row.arr_period,
                    row.other_kwargs,
                    )
            self.info_program[key] = obj_program

    def traverse_the_alarm_program(self, only_once):
        ''' 遍历报警程序
        报警算法的返回值：
            None or tuple
                ((stock_code, period, s_now, message), ...)
        '''
        flag = False
        now = datetime.datetime.now()
        s_now = now.strftime('%Y-%m-%d %H:%M')
        self.arr_alarm_msg = []
        df_alarm_message = self.obj_DataTable.read_db__alarm_message()
        for alarm_program in self.info_program.values():
            arr_msg = alarm_program.run(s_now, only_once)
            if arr_msg:
                # 去除重复数据
                arr_msg = self.check_repeat(df_alarm_message, arr_msg)
                self.arr_alarm_msg.extend(arr_msg)
        if self.arr_alarm_msg:
            df_msg = self.save_alarm_message()
            self.output_alarm_msg(df_msg)
            flag = True
            logger.debug(f'flag: {flag}, arr_alarm_msg: {self.arr_alarm_msg}')
        logger.debug(f'traverse_the_alarm_program() ... s_now: {s_now}, flag: {flag}, run time: {(datetime.datetime.now() - now).total_seconds()}s')
        return flag

    def check_repeat(self, df_alarm_message, arr_msg):
        ''' 输出报警信息 '''
        arr_msg_new = []
        for record in arr_msg:
            stock_code, period, s_now, message = record
            label = (stock_code, period, s_now)
            if label not in df_alarm_message.index:
                arr_msg_new.append(record)
        return tuple(arr_msg_new)

    def save_alarm_message(self):
        arr_column = ['s_now', 'stock_code', 'period', 'message']
        index_name = ['s_now', 'stock_code', 'period']
        df = pd.DataFrame(self.arr_alarm_msg, columns=arr_column)
        df.set_index(index_name, inplace=True)
        self.obj_DataTable.save_db__alarm_message(df)
        logger.debug(f'save_alarm_message() ...\n{df}')
        return df

    def output_alarm_msg(self, df_msg):
        ''' 输出报警信息 '''
        print(df_msg)


class PeriodType:
    ''' k线周期 转 pandas时间类型 '''
    pattern = re.compile(r'(?P<count>\d+)(?P<key>\w)')
    cls_type = {
            's': Second,
            'm': Minute,
            'h': Hour,
            'd': Day,
            }
    period = None
    rule = None

    def __init__(self, period):
        self.period = period
        m = self.pattern.match(period)
        if m:
            info = m.groupdict()
            count = int(info['count'])
            cls_time= self.cls_type[ info['key'] ]
            self.rule = cls_time(count)
        else:
            raise ValueError(f're.search() fail. {period}')

    def get_rule(self):
        return self.rule


class SingleStockInfo:
    ''' 单个股票信息
    报警信息, info_alarm, dict
    {
            # k线周期，例如: '1m', '5m', ...
            period: {
                    # pandas.DataFrame.resample()的rule参数
                    'rule': k线周期转换规则,
                    # 报警对象的集合
                    'arr_alarm': set([报警对象_01, 报警对象_02, ...]),
                    },
            ...
            }
    k线数据, data_kline, dict
    {
            '1m': 1分钟k线数据,
            '5m': 1分钟k线数据,
            ...
            }
    '''
    period_base = '1m'
    # 股票代码
    stock_code = None
    # 股票名称
    stock_name = None
    # 数据表名
    table_name = None
    # 数据库
    obj_db = None
    # 数据源
    obj_source = None
    # 报警信息
    info_alarm = None
    # k线数据
    data_kline = None
    # 限制k线数据的长度(1年 = 52周 * 5天 * 4小时 * 60分钟)
    limit_size = 62400

    def __init__(
            self, stock_code, stock_name, period_base, obj_db, obj_source
            ):
        ''' 实例初始化
            stock_code          股票代码
            stock_name          股票名称
            obj_source          行情数据源
        '''
        self.period_base = period_base
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.table_name = f'{stock_code}_today'
        self.obj_db = obj_db
        self.obj_source = obj_source
        df = self.get_bars_history(True)
        self.data_kline = {period_base: df}

    def get_bar(self, period):
        ''' 获取k线数据 '''
        return self.data_kline[period]

    def get_last_date(self, period):
        ''' 最后一个k线数据的时间 '''
        return self.data_kline[period].index[-1]

    def get_bars_history(self, flag_limit_size=False):
        ''' 获取历史数据
            从数据源获取数据失败，raise ValueError()
        '''
        try:
            # 从数据表读取数据
            df = self.read_data_from_database()
        except (sqlalchemy.exc.OperationalError, ValueError):
            # 从行情数据源获取数据
            df = self.read_data_from_QuotesDataSource()
        if flag_limit_size and self.limit_size < df.index.size:
            df = df[-self.limit_size:]
        return df

    def get_bars_new(self):
        ''' 从行情数据源更新单个k线的数据
        返回值:
            df_new                  下载的k线数据
            raise ValueError()      从数据源获取数据失败
        '''
        period = self.period_base
        start_date = self.get_last_date(period)
        df_new = self.obj_source.get_data_missing(
                self.stock_code, period, start_date
                )
        df_new = self.data_merge(period, df_new, True)
        return df_new

    def data_merge(self, period, df_new):
        ''' 数据合并 '''
        df_old = self.data_kline[period]
        # 去除重复数据
        df = self.data_deduplication(df_old, df_new)
        logger.debug(f'data_merge() ... {self.stock_code}, df_old.index[-1]: {df_old.index[-1]}, df.empty: {df.empty}')
        if not df.empty:
            # 数据合并
            logger.debug(f'\tdf.index.size: {df.index.size}, df.index[0]: {df.index[0]}')
            self.data_kline[period] = pd.concat([df_old, df])
        return df

    def read_data_from_database(self):
        ''' 从数据库读取历史数据 '''
        try:
            df = self.obj_db.read_db__kline(self.table_name)
        except ValueError as e:
            logger.info(f'{e}')
            df = None
            self.obj_db.table_create__kline(self.table_name)
        if df is None or df.empty:
            year = datetime.date.today().year
            t_name = f'{self.stock_code}_{year}'
            df = self.obj_db.read_db__kline(t_name)
        return df

    def read_data_from_QuotesDataSource(self, n_bars=None):
        ''' 从行情源读取历史数据 '''
        df = self.obj_source.get_data_once(
                self.stock_code, self.period_base, count=n_bars
                )
        if not df.empty:
            # 下载数据，写入数据表
            self.obj_db.save_db__kline(df, self.table_name)
        return df

    def data_deduplication(self, df_old, df_new):
        ''' 去除重复数据 '''
        if not df_old.empty and df_old.index[-1] in df_new.index:
            index_name = df_old.index.name
            df = df_new.loc[df_old.index[-1] < df_new.index]
            df.index.rename(index_name, inplace=True)
        else:
            df = df_new
        return df

    def get_today_data(self, s_date=None):
        ''' 今天的数据
            s_date           指定日期
                None or str
        '''
        df = self.data_kline[self.period_base]
        if s_date is None:
            today = datetime.date.today()
        else:
            today = s_date
        df_today = df.loc[pd.Timestamp(today) <= df.index]
        return df_today

    def save_to_history(self, df_today):
        ''' 今天的数据保存到历史表
            s_date           指定日期
                None or pandas.Timestamp
        '''
        year = datetime.date.today().year
        if not df_today.empty:
            t_name = f'{self.stock_code}_{year}'
            if not self.obj_db.table_is_exists(t_name):
                self.obj_db.table_create(t_name)
            self.obj_db.save_db__kline(df_today, t_name)

    def save_today_data_to_csv(self):
        ''' 今天的数据保存到csv (mt5格式) '''
        df_today = self.get_today_data()
        today = datetime.date.today()
        if to_csv_mt5:
            f_name = os.path.join(
                    settings.dir_data, f'{self.stock_code}_{today}.csv'
                    )
            to_csv_mt5(df_today, f_name)

    def period_conversion(self, period, df_base=None, func_name=None):
        ''' k线周期数据的转换
            period          k线周期
            df_base         转换前的k线数据
            func_name       k线周期转换的函数名称
        '''
        obj_PeriodType = PeriodType(period)
        rule = obj_PeriodType.get_rule()
        if df_base is None:
            df_base = self.data_kline[self.period_base]
        if func_name is None:
            func_name = dict(open='first', high='max', low='min', close='last')
        df = df_base.resample(rule=rule).agg(func_name).dropna()
        return df

    def period_add(self, period):
        ''' 增加k线周期数据 '''
        if period not in self.data_kline:
            df = self.period_conversion(period)
            self.data_kline[period] = df

    def period_remove(self, period):
        ''' 删除k线周期数据 '''
        if period in self.data_kline:
            del self.data_kline[period]

    def period_update(self):
        ''' 更新其它的k线周期数据
            period_base周期，使用get_bars_new()
        '''
        arr_period = set(self.data_kline.keys())
        arr_period.remove(self.period_base)
        for period in arr_period:
            df_base = self.data_kline[self.period_base]
            df_old = self.data_kline[period]
            start_date = df_old.index[-1]
            df_base_new = df_base.loc[start_date <= df_base.index]
            df_new = self.period_conversion(period, df_base=df_base_new)
            self.data_kline[period] = pd.concat([df_old[:-1], df_new])


class QuotesDataSource(ABC):
    ''' 行情数据源 '''
    name_source_en = None
    name_source_zh = None
    username = None
    password = None

    def __init__(self, obj_db, name_source, name_source_zh=None):
        df = obj_db.read_db__QuotesDataSource_account()
        if name_source not in df.index:
            msg = f'行情源账号信息不存在, name_source={name_source}'
            logger.error(msg)
            raise ValueError(msg)
        row = df.loc[name_source]
        self.username = row.username
        self.password = row.password
        self.name_source = name_source
        self.name_source_zh = name_source_zh

    def set_time_left(self, df, offset_time=Minute(1)):
        ''' df索引，由"结束时间"转为"开始时间" '''
        index_name = df.index.name
        df.index = df.parallel_apply(lambda o: o.name - offset_time, axis=1)
        df.index.rename(index_name, inplace=True)

    def set_time_right(self, obj, offset_time=Minute(1)):
        ''' df索引，由"开始时间"转为"结束时间" '''
        assert (
                isinstance(obj, datetime.datetime)
                or isinstance(obj, pd.Timestamp)
                or isinstance(obj, pd.DataFrame)
                ), 'obj类型错误'

        if (
                isinstance(obj, datetime.datetime)
                or isinstance(obj, pd.Timestamp)
                ):
            ret = obj + offset_time
        else:
            index_name = obj.index.name
            obj.index = obj.parallel_apply(lambda o: o.name + offset_time, axis=1)
            obj.index.rename(index_name, inplace=True)
            ret = obj
        return ret

    @abstractmethod
    def connect_server(self):
        ''' 连接服务器 '''
        pass

    @abstractmethod
    def is_auth(self):
        ''' 查询是否登录/连接成功 '''
        pass

    @abstractmethod
    def get_data_once(self, stock_code, period, end_time=None, count=1):
        ''' 获取单个股票的历史数据
        入口参数
            stock_code              股票代码
            period                  k线周期
            end_time                查询的截止时间，支持的类型为datetime.datetime或None。
                None                    jqdatasdk默认为datetime.datetime.now()
            count                   记录数量
        返回值
            info        pandas.DataFrame
        '''
        pass

    @abstractmethod
    def get_data(self, stock_code, period, next_time=None):
        ''' 获取单个股票的历史数据
        入口参数
            stock_code              股票代码
            period                  k线周期
            next_time               数据截止时间
        返回值
            info        pandas.DataFrame
        '''
        pass

    @abstractmethod
    def get_data_missing(self, str_or_list, period, start_date, end_date=None):
        ''' 下载缺失的数据
        ━━━┻━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━→→    时间轴
          最后时间              现在

        入口参数:
            str_or_list             股票代码 or list
            period                  k线周期
            start_date              数据开始时间
            end_date                数据结束时间
        返回值:
            单个股票    DataFrame
            多个股票    dict
                {
                        股票代码_01: DataFrame,
                        ...
                        }
        '''
        pass


class JqData(QuotesDataSource):
    ''' 聚宽量化交易平台
    聚宽数据的k线时间，为结束时间。
        1m的2020-10-10T09:31 对应于 2020-10-10T09:30:00.000 --- 09:30:59.999
        5m的2020-10-10T09:35 对应于 2020-10-10T09:30:00.000 --- 09:34:59.999
        >>> datas[:2]
                                  open       high        low      close
        date
        2020-10-14 09:31:00  4830.4575  4830.4575  4819.1404  4819.3108
        2020-10-14 09:32:00  4816.8566  4819.1151  4816.2843  4819.1151
        >>>
    '''

    def __init__(self, obj_db):
        super().__init__(
                obj_db, name_source='JoinQuant',
                name_source_zh='聚宽量化交易平台'
                )

    def connect_server(self):
        try:
            jqdatasdk.auth(self.username, self.password)
        except Exception:
            raise ValueError('聚宽服务器连接失败！')

    def is_auth(self):
        return jqdatasdk.is_auth()

    def get_data_once(self, stock_code, period, end_time=None, count=1):
        ''' 获取单个股票的历史数据，限制长度5000条记录 '''
        if end_time is None:
            end_time = datetime.datetime.now()
        if not (isinstance(count, int) and 0 < count <= 5000):
            count = 5000
        if not self.is_auth():
            self.connect_server()
        df = jqdatasdk.get_bars(
                security=stock_code,
                count=count,
                unit=period,
                fields=['date', 'open', 'high', 'low', 'close'],
                include_now=False,
                end_dt=end_time,
                fq_ref_date=None,
                df=True
                )
        if df.empty:
            raise ValueError(f'未能获取数据; {stock_code}, {period}')
        index_name = 'date'
        df.set_index(index_name, inplace=True)
        self.set_time_left(df, Minute(1))
        return df

    def get_data(self, stock_code, period, next_time=None):
        ''' 获取单个股票的历史数据 '''
        arr_data = []
        try:
            while True:
                df = self.get_data_once(stock_code, period, next_time, 5000)
                arr_data.append(df)
                next_time = df.index[0].to_pydatetime()
        except ValueError as e:
            pass
        if arr_data:
            info = pd.concat(arr_data) 
            info.sort_index(inplace=True)
        else:
            raise ValueError(f'未能获取数据; {self.stock_code}, {self.period}')
        return info

    def get_data_missing(
            self, str_or_list, period, start_date, end_date=None,
            offset_right=True,
            ):
        ''' 下载缺失的数据
        ━━━┻━━━━━━━━━━━━━━━━━━━━━┻━━━━━━━━━━→→    时间轴
          最后时间              现在

        入口参数:
            str_or_list             股票代码 or list
            period                  k线周期
            start_date              数据开始时间
            end_date                数据结束时间
            offset_right            时间向右偏移
                k线数据的时间，由"开始时间"转为"结束时间"。
        返回值:
            单个股票    DataFrame
            多个股票    dict
                {
                        股票代码_01: DataFrame,
                        ...
                        }
        '''
        # 参数检测
        assert (
                isinstance(start_date, str)
                or isinstance(start_date, datetime.datetime)
                ), 'start_date值错误'
        assert (
                end_date is None
                or isinstance(end_date, str)
                or isinstance(end_date, datetime.datetime)
                ), 'end_date值错误'
        # 数据准备
        start_date, end_date = self._data_prepare(
                start_date, end_date, offset_right
                )
        arr_field = ['open', 'high', 'low', 'close']
        logger.debug(f'str_or_list: {str_or_list}, start_date: {start_date}, end_date: {end_date}, period: {period}, ')
        # 数据下载
        if not self.is_auth():
            self.connect_server()
        df = jqdatasdk.get_price(
                security=str_or_list,
                start_date=start_date,
                end_date=end_date,
                frequency=period,
                fields=arr_field,
                skip_paused=True,
                panel=False,
                )
        logger.debug(f'download record: {df.index.size}, df:\n{df}')
        if df.empty:
            raise ValueError(f'未能获取数据. {str_or_list}, {period}')
        # 格式转换
        ret = self._data_format_change(str_or_list, df)
        return ret

    def _data_prepare(self, start_date, end_date, offset_right):
        ''' get_data_missing() 数据准备 '''
        now = datetime.datetime.now()
        if isinstance(start_date, str):
            start_date = dateutil.parser.parse(start_date)
        if (
                now.date() == start_date.date()
                and datetime.datetime(now.year, now.month, now.day, 15)
                        < start_date
                ):
            raise ValueError(f'start_date值错误 {start_date} {str_or_list}')
        if offset_right:
            start_date = self.set_time_right(start_date)
        if end_date is None:
            end_date = now
        elif isinstance(end_date, str):
            end_date = dateutil.parser.parse(end_date)
            if offset_right:
                start_date = self.set_time_right(start_date)
        return start_date, end_date
 
    def _data_format_change(self, str_or_list, df):
        ''' get_data_missing() 下载的数据，进行格式转换 '''
        index_name = 'date'
        if isinstance(str_or_list, str):
            # 单个个股票
            self.set_time_left(df, Minute(1))
            df.index.rename(index_name, inplace=True)
            ret = df
        else:
            # 多个股票
            df.set_index(['code', 'time'], inplace=True)
            info = {}
            for code in str_or_list:
                df_code = df.loc[code]
                df_code.index.rename(index_name, inplace=True)
                self.set_time_left(df_code, Minute(1))
                info[code] = df_code
            ret = info
        return ret


class SingleAlarmProgram:
    ''' 单个报警程序
    报警程序的文件名：
        src/plugins/xxx.py
        其中，xxx 是alarm_program_info数据表的algorithm字段内容。
    报警程序中，报警算法的固定函数名：alarm_algorithm()
        遍历n个股票代码:
            遍历n个k线周期:
                执行一个报警算法
    1) 单个k线周期:
        算法:
            macd的diff上穿dea or macd的diff下穿dea
        例子:
            股票代码                k线周期
            --------------------------------------
            eur/usd                 (1小时,)
            中证小盘500指数         (1分钟, 5分钟)
            沪深300指数             (1分钟, 5分钟)
        入口参数:
            1) eur/usd
                arr_stock_code = ['eur/usd']
                arr_period = ['1h']
                other_kwargs = None
            2) 中证小盘500指数, 沪深300指数
                arr_stock_code = ['000905.XSHG', '000300.XSHG']
                arr_period = ['1m', '5m']
                other_kwargs = None
    2) 多个k线周期:
        算法:
            长周期k线的macd的0 < diff and 短周期k线的diff上穿dea
            or 长周期k线的macd的diff < 0 and 短周期k线的diff下穿dea
        例子:
            股票代码            k线周期(长周期, 短周期)
            -------------------------------------------
            eur/usd             (4小时, 1小时)
            中证小盘500指数     (30分钟, 5分钟)
            沪深300指数         (30分钟, 5分钟)
        入口参数:
            1) eur/usd
                arr_stock_code = ['eur/usd']
                arr_period = ['1h']
                other_kwargs = {'period_long': '4h', 'period_short': '1h'}
            2) 中证小盘500指数
                arr_stock_code = ['000905.XSHG', '000300.XSHG']
                arr_period = ['5m']
                other_kwargs = {'period_long': '30m', 'period_short': '5m'}
        注意:
            算法包含多个k线周期时，arr_period选最小的k线周期。
    info_program        报警程序的信息，由KlineInfo.get_alarm_info()设置。
    {
            'algorithm': row.algorithm,
            'arr_stock_code': arr_stock_code,
            'arr_period': arr_period,
            'other_kwargs': other_kwargs,
            'remark': row.remark,
            'data_kline': obj_code.data_kline
            }
    '''
    # 报警算法函数
    algorithm = None
    # 报警输出信息
    arr_alarm_msg = None
    # 最后运行时间，减少计算量
    info_last_time_run = None
    # 最后的报警信息，防止重复报警
    info_alarm_msg = None
    # 报警程序的信息，由KlineInfo.get_alarm_info()设置。
    info_program = None

    def __init__(self, info):
        ''' 返回值: None or ValueError '''
        self.info_alarm_msg = {}
        self.info_last_time_run = {}
        self.info_program = info
        # 报警算法函数
        module_name = f'{settings.dir_plugin}.{info["algorithm"]}'
        spec = importlib.util.find_spec(module_name)
        if spec is None:
            raise ValueError(f'报警程序{module_name}不存在')
        obj_module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(obj_module)
        try:
            self.algorithm = obj_module.alarm_algorithm
        except AttributeError:
            msg = f'报警程序{module_name}中，没有报警函数alarm_algorithm()'
            logger.error(msg)
            raise ValueError(msg)

    def run(self, s_now, only_once):
        ''' 定时执行
        入口参数:
            s_now           程序启动时间
            only_once       第一次运行的标志
        返回值: tuple

        self.algorithm()的返回值: ValueError or list
            [(stock_code, period, s_now, message), ...]
        '''
        # 当天k线周期的时间序列
        info_time_range = {}
        # 报警的信息内容
        arr_alarm_msg = []
        # 一个报警算法 ---> n个股票代码 ---> n个k线周期
        for stock_code in self.info_program['arr_stock_code']:
            for period in self.info_program['arr_period']:
                if not self.check_run_time(only_once, s_now, info_time_range):
                    # not (第0次运行 or 第[1..n)次运行 and now在时间序列df_ts中)
                    continue
                alarm_algorithm = self.info_program['algorithm']
                label = (stock_code, period)
                s_last_time = self.info_last_time_run.get(label)
                info = {
                        'stock_code': stock_code,
                        'period': period,
                        'other_kwargs': self.info_program['other_kwargs'],
                        'remark': self.info_program['remark'],
                        'data_kline': self.info_program['data_kline'],
                        # 上次运行时间
                        's_last_time': s_last_time,
                        # 本次运行时间
                        's_now': s_now,
                        }
                logger.debug(f'alarm_algorithm: {alarm_algorithm}, label: {label}, s_last_time: {s_last_time}, s_now: {s_now}')
                try:
                    arr_cross = self.algorithm(info)
                    logger.debug(f'arr_cross: {arr_cross}')
                except ValueError as e:
                    logger.info(f'{e}')
                    continue
                if arr_cross and self.info_alarm_msg.get(label) != s_now:
                    # 记录上次报警时间，防止重复报警
                    self.info_alarm_msg[label] = s_now
                    arr_alarm_msg.extend(arr_cross)
                # 记录本次算法的运行时间
                self.info_last_time_run[label] = s_now
        logger.debug(f'arr_alarm_msg: {arr_alarm_msg}')
        return arr_alarm_msg

    def get_today_time_range(self, period, info_ts):
        ''' 获取当天的时间序列 '''
        if period in info_ts:
            arr_ts = info_ts[period]
        else:
            today = datetime.date.today()
            tomorrow = today + datetime.timedelta(days=1)
            rule = PeriodType(period).get_rule()
            arr_ts = pd.date_range(today, tomorrow, freq=rule, closed='left')
            info_ts[period] = arr_ts
        return arr_ts

    def check_run_time(self, only_once, s_now, info_time_range):
        ''' 第0次运行 or 第[1..n)次运行 and now在时间序列df_ts中 '''
        if only_once:
            # 第0次运行
            flag = True
        else:
            # 第[1..n)次运行
            now = pd.Timestamp(s_now)
            period = self.info_program['period']
            if period in info_time_range:
                df_ts = info_time_range[period]
            else:
                today = datetime.date(now.year, now.month, now.day)
                tomorrow = today + datetime.timedelta(days=1)
                df_ts = pd.date_range(
                        today, tomorrow, freq=period, name='date', closed='left'
                        )
                info_time_range[period] = df_ts
            # 检查: now在时间序列df_ts中
            flag = now in df_ts
        logger.debug(f'check_run_time(): ... flag: {flag}')
        return flag


def init_program():
    ''' 程序初始化 '''
    obj_db = DataTable()
    obj_db.table__init()
    logger.info('创建数据表 ... 完成')


def main():
    logger.debug('main() ...')
    obj_TS = TimingStart()
    obj_TS.event_timer()


if __name__ == '__main__':
    main()

