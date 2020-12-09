# -*- encoding: utf-8 -*-
''' 报警条件：macd的diff和dea交叉 
算法使用多个k线周期: 
    other_kwargs is None 为 False，忽略 period。
算法使用单个k线周期:
    other_kwargs is None 为 True，使用 period。
'''

import pandas as pd
import talib


class MacdCross:
    ''' 报警条件：macd的diff和dea交叉 '''
    stock_code = None
    period = None
    other_kwargs = None
    remark = None
    data_kline = None
    s_last_time = None
    s_now = None

    def __init__(self, info):
        ''' 报警条件：macd的diff和dea交叉
        info.keys():
            stock_code              股票代码
            period                  k线周期
            other_kwargs            算法的参数
            remark                  算法的备注
            data_kline              单个股票的所有数据
            s_last_time             上次运行时间，None or str
                "2020-11-14 21:16"
            s_now                   本次运行时间，str
                "2020-11-14 21:17"
        '''
        self.stock_code = info['stock_code']
        self.period = info['period']
        self.other_kwargs = info['other_kwargs']
        self.remark = info['remark']
        self.data_kline = info['data_kline']
        self.s_last_time = info['s_last_time']
        self.s_now = info['s_now']

    def run(self):
        ''' 报警算法
        返回值:     ValueError or list
            [
                    (stock_code, period, s_now, message),
                    ...
                    ]
        '''
        # 获取参数
        price = self.get_price()
        # 计算macd
        df_macd = self.calc_macd(price)
        # 检查交叉
        arr_cross = self.check_cross(df_macd)
        return arr_cross

    def get_price(self):
        ''' 获取价格 '''
        df = self.data_kline[self.period]
        if isinstance(self.other_kwargs, dict):
            price_type = self.other_kwargs.get('price_type')
        else:
            price_type = None
        if price_type is None:
            # 缺省使用 "开盘价"，减少计算量
            price = df['open']
        else:
            price = df[price_type]
        return price

    def calc_macd(self, price):
        ''' 计算macd '''
        arr_name = ['DIFF', 'DEA', 'BAR']
        arr_data = talib.MACD(price.values, 26, 12, 9)
        df = pd.DataFrame(
                dict(zip(arr_name, arr_data)), index=price.index, columns=arr_name
                )
        df.dropna(inplace=True)
        if self.s_last_time:
            df_2 = df.loc[pd.Timestamp(self.s_last_time) < df.index]
            if df_2.empty:
                raise ValueError(f"数据未更新. s_now: {self.s_now}, {self.stock_code}, {self.period}")
            elif df_2.index.size == 1:
                df_macd = df[-2:]
            else:
                df_macd = df_2
        else:
            df_macd = df
        return df_macd

    def check_cross(self, df_macd):
        ''' 检查diff、dea的交叉 '''
        arr_cross = []
        for i in range(df_macd.index.size - 1):
            j = i + 1
            if (df_macd.BAR[i] < 0 and 0 < df_macd.BAR[j]):
                msg = '上交叉'
            elif (0 < df_macd.BAR[i] and df_macd.BAR[j] < 0):
                msg = '下交叉'
            else:
                msg = None
            if msg:
                record = (
                        df_macd.index[j].strftime('%Y-%m-%d %H:%M'),
                        self.stock_code,
                        self.period,
                        msg,
                        )
                arr_cross.append(record)
        return arr_cross


def alarm_algorithm(info):
    ''' 报警算法
    入口参数, dict
        stock_code              股票代码
        period                  k线周期
        other_kwargs            算法的参数
        remark                  算法的备注
        data_kline              单个股票的所有数据
        s_last_time             上次运行时间，None or str
            "2020-11-14 21:16"
        s_now                   本次运行时间，str
            "2020-11-14 21:17"
    返回值, ValueError or list
        [
                (s_now, stock_code, period, message),
                ...
                ]
    '''
    obj = MacdCross(info)
    arr_cross = obj.run()
    return arr_cross

