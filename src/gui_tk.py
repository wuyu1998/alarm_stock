# -*- encoding: utf-8 -*-
''' Tkinter图形界面
'''

import datetime
import os
import pandas as pd
import tkinter as tk
import tkinter.ttk as ttk
import vlc

import alarm_stock as a_s
import settings as settings

class Application(ttk.Frame):
    # 闭市后，监控程序继续运行的时间
    t_continue_run = datetime.timedelta(seconds=settings.n_continue_run)
    # k线数据对象实例 (用在所有的报警算法中)
    obj_KlineInfo = None
    # vlc
    obj_sound = None
    # 仅第一次运行
    only_once = None

    def __init__(self, master=None):
        super().__init__(master)
        self.only_once = True
        self.init_alarm_program()
        self.create_UI()
        self.update_clock()
        self.load_message()

    def update_clock(self):
        ''' 更新定时器 '''
        self.job()
        now = datetime.datetime.now()
        delta_seconds, delta_microseconds = self.calc_delta_time(now)
        delta_time = int(delta_seconds * 1000 + delta_microseconds / 1000.0)
        x_after = self.master.after(delta_time, self.update_clock)

    def calc_delta_time(self, now):
        ''' 计算休眠时间 '''
        market_opening = False
        now_weekday = now.date().weekday()
        t_am_begin = datetime.datetime(now.year, now.month, now.day, 9, 30)
        t_am_end = datetime.datetime(now.year, now.month, now.day, 11, 30)
        t_pm_begin = datetime.datetime(now.year, now.month, now.day, 13)
        t_pm_end = datetime.datetime(now.year, now.month, now.day, 15, 0)
        if now_weekday < 5:
            if now < t_am_begin:
                # 早市之前
                next_time = t_am_begin
            elif t_am_end < now < t_pm_begin:
                # 中午休市时间
                next_time = t_pm_begin
            elif (
                    t_am_begin <= now <= t_am_end + self.t_continue_run
                    or t_pm_begin <= now <= t_pm_end + self.t_continue_run
                    ):
                # 早上开市期间 or 下午开市期间
                next_time = datetime.datetime(
                        now.year, now.month, now.day, now.hour, now.minute, 3,
                        ) + datetime.timedelta(minutes=1)
                market_opening = True
            else:
                # 下午闭市之后
                if now_weekday == 4:
                    # 周五
                    wait_day = datetime.timedelta(days=3)
                else:
                    # 周一..周四
                    wait_day = datetime.timedelta(days=1)
                next_time = tm_am_begin + wait_day
        else:
            # 周末
            wait_day = datetime.timedelta(days=7 - now_weekday)
            next_time = tm_am_begin + wait_day
        delta_time = next_time - now
        return (delta_time.seconds, delta_time.microseconds)

    def init_audio(self):
        ''' 初始化声音系统 '''
        f_name = settings.f_name_audio
        if not os.path.exists(f_name):
            msg = "声音文件不存在"
            raise ValueError(msg)
        self.obj_sound = vlc.MediaPlayer(f_name)

    def play_audio(self):
        self.obj_sound.stop()
        self.obj_sound.play()

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

    def init_alarm_program(self):
        ''' 初始报警程序 '''
        self.only_once = True
        self.obj_KlineInfo = a_s.KlineInfo()
        self.init_audio()

    def create_UI(self):
        ''' 初始化窗口控件 '''
        self.gui_alarm_program()
        self.gui_alarm_message()
        self.grid(sticky=(tk.N, tk.S, tk.W, tk.E))
        self.grid_rowconfigure(0, weight=1)
        self.grid_columnconfigure(0, weight=1)

    def gui_alarm_program(self):
        ''' 界面: 报警程序 '''
        tp = ttk.Treeview(self, columns='capital')
        df_stock_code = self.obj_KlineInfo.obj_DataTable.read_db__stock_code()
        for key, obj_program in self.obj_KlineInfo.info_program.items():
            info = obj_program.info_program
            # 文件名
            program_name = tp.insert(
                    '', 'end', text=info['algorithm'], values=info['remark'],
                    )
            # 监控的股票
            stock_info = tp.insert(program_name, 'end', text='股票信息')
            for stock_code in info['arr_stock_code']:
                tp.insert(
                        stock_info, 'end', text=stock_code,
                        values=df_stock_code.loc[stock_code].display_name,
                        )
            # k线周期
            period_info = tp.insert(program_name, 'end', text='k线周期')
            for period in info['arr_period']:
                tp.insert(period_info, 'end', text=period)
            # 附加信息
            other_info = tp.insert(program_name, 'end', text='附加信息')
            for key, value in info['other_kwargs'].items():
                tp.insert(other_info, 'end', text=key, values=value)
        tp.grid(column=1, row=0, sticky=(tk.N, tk.S, tk.W, tk.E))
        self.tree_program = tp

    def gui_alarm_message(self):
        ''' 界面: 报警信息 '''
        tm = ttk.Treeview(self, columns='capital')
        y_scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=tm.yview)

        y_scrollbar.grid(column=0, row=1, sticky=(tk.N, tk.W, tk.E, tk.S))
        tm['columns'] =('stock_code', 'period', 'message')
        tm.heading('#0', text='报警时间', anchor='w')
        tm.column('#0', anchor='w')
        tm.heading('stock_code', text='股票代码')
        tm.column('stock_code', anchor='e')
        tm.heading('period', text='k线周期')
        tm.column('period', anchor='e')
        tm.heading('message', text='信息')
        tm.column('message', anchor='e')
        tm.grid(column=1, row=1, sticky=(tk.N, tk.S, tk.W, tk.E))
        tm.configure(yscrollcommand=y_scrollbar.set)

        self.table_message = tm
        self.y_scrollbar = y_scrollbar

    def load_message(self):
        ''' 读取数据表: 报警信息
        只显示当天的记录。
        '''
        today = datetime.date.today()
        df = self.obj_KlineInfo.obj_DataTable.read_db__alarm_message()
        df.sort_index(inplace=True)
        for k, v in df.iterrows():
            s_now, stock_code, period = k
            if pd.Timestamp(s_now).date() != today:
                continue
            self.table_message.insert(
                    '', 0, text=s_now, values=(stock_code, period, v.message)
                    )


def main():
    root = tk.Tk()
    app = Application(root)
    root.mainloop()


if __name__ == '__main__':
    main()

