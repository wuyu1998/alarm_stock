# -*- encoding: utf-8 -*-
''' 更新当天数据
'''

import argparse
import datetime
import os

import settings
from alarm_stock import KlineInfo, logger, to_csv_mt5


def update_data_today(
        s_date=None, save_db=True, save_csv=True, download_data=True
        ):
    ''' 更新当天数据
        s_date          指定日期
            None or str
        save_db         写入历史数据表
        save_csv        写入csv文件 (mt5格式)
        download_data   下载最新的数据
    '''
    if not (save_db or save_csv):
        return
    if s_date is None:
        s_date = datetime.date.today().isoformat()
    obj_KlineInfo = KlineInfo()
    if download_data:
        obj_KlineInfo.download_new_data()
    for obj_SingleStockInfo in obj_KlineInfo.info_stock.values():
        try:
            df_today = obj_SingleStockInfo.get_today_data(s_date)
            if save_db:
                obj_SingleStockInfo.save_to_history(df_today)
            if save_csv:
                f_name = os.path.join(
                        settings.dir_data,
                        f'{obj_SingleStockInfo.stock_code}_{s_date}.csv'
                        )
                to_csv_mt5(df_today, f_name)
        except ValueError as e:
            logger.error(f'{e}')


def proc_parser():
    parser = argparse.ArgumentParser(description='补充历史数据')
    parser.add_argument(
            '-d', '--date', type=str, help='处理的日期', metavar='YYYY-MM-DD',
            default=None,
            )
    parser.add_argument(
            '-db', '--save_db', action='store_true',
            help='写入历史数据表',
            )
    parser.add_argument(
            '-csv', '--save_csv', action='store_true',
            help='写入csv文件 (mt5格式)',
            )
    parser.add_argument(
            '--download', action='store_true', help='下载最新的数据',
            )
    res = parser.parse_args()
    return res


def main():
    print('-' * 40)
    res = proc_parser()
    update_data_today(
            s_date=res.date,
            save_db=res.save_db,
            save_csv=res.save_csv,
            download_data=res.download,
            )


if __name__ == '__main__':
    main()
