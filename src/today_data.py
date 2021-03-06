# -*- encoding: utf-8 -*-
''' 更新当天数据
'''

import argparse
import datetime
import os

import settings
from alarm_stock import KlineInfo, logger, to_csv_mt5


def update_data_today(
        s_date=None, save_csv=True, download_data=True, after_date=None,
        ):
    ''' 更新当天数据
        s_date          指定日期
            None or str
        save_csv        写入csv文件 (mt5格式)
        download_data   下载最新的数据
        after_date      导出指定日期之后的数据，csv格式
    '''
    s_today = datetime.date.today().isoformat()
    if after_date is None and s_date is None:
        s_date = s_today
    obj_KlineInfo = KlineInfo()
    if download_data:
        obj_KlineInfo.download_new_data()
    for obj_SingleStockInfo in obj_KlineInfo.info_stock.values():
        if download_data:
            obj_SingleStockInfo.save_to_history()
        try:
            if after_date is None:
                if save_csv:
                    df_today = obj_SingleStockInfo.get_today_data(s_date)
                    f_name = os.path.join(
                            settings.dir_data,
                            f'{obj_SingleStockInfo.stock_code}_{s_date}.csv'
                            )
                    to_csv_mt5(df_today, f_name)
            else:
                df = obj_SingleStockInfo.get_after_data(after_date)
                f_name = os.path.join(
                        settings.dir_data,
                        f'{obj_SingleStockInfo.stock_code}_{s_today}.csv'
                        )
                to_csv_mt5(df, f_name)
        except ValueError as e:
            logger.error(f'{e}')


def proc_parser():
    parser = argparse.ArgumentParser(description='补充历史数据')
    parser.add_argument(
            '-d', '--date', type=str, metavar='YYYY-MM-DD', default=None,
            help='处理的日期（缺省值None，当天）',
            )
    parser.add_argument(
            '-a', '--after_date', type=str, metavar='YYYY-MM-DD', default=None,
            help='导出指定日期之后的数据，csv格式（忽略--date）',
            )
    parser.add_argument(
            '-csv', '--save_csv', action='store_true',
            help='写入csv文件（mt5格式）',
            )
    parser.add_argument(
            '--download', action='store_true', help='下载最新的数据',
            )
    res = parser.parse_args()
    return res


def main():
    print('-' * 40)
    res = proc_parser()
    # print(f'res: {res}')
    update_data_today(
            s_date=res.date,
            save_csv=res.save_csv,
            download_data=res.download,
            after_date=res.after_date,
            )


if __name__ == '__main__':
    main()
