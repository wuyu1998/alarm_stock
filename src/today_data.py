# -*- encoding: utf-8 -*-
''' 更新当天数据
'''

import argparse
import os

from alarm_stock import KlineInfo, logger, to_csv_mt5


def update_data_today(
        s_date=None, save_db=True, save_csv=True, download_data=True
        ):
    ''' 更新当天数据
        s_date          指定日期
            None or str
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


def main_2():
    parser = argparse.ArgumentParser(description='更新当天数据')
    parser.add_argument('--s_date', action='s_date', default=None)
    parser.add_argument('--save_db', action='save_db', default=True)
    parser.add_argument('--save_csv', action='save_csv', default=True)
    parser.add_argument('--download_data', action='download_data', default=True)
    res = parser.parse_args()
    update_data_today(
            s_date=res.s_date,
            save_db=res.save_db,
            save_csv=res.save_csv,
            download_data=res.download_data,
            )


def main():
    update_data_today()


if __name__ == '__main__':
    main()
