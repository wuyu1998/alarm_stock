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
    # 报警程序运行标志
    flag_run = None
    # 标识符: tk.after()
    id_after = None
    # 股票代码表
    df_stock_code = None

    def __init__(self, master=None):
        super().__init__(master)
        self.create_UI()
        self.init_audio()
        self.init_alarm_program()
        self.load_data()

    def create_UI(self):
        ''' 初始化窗口控件 '''
        self.style = ttk.Style(self.master)
        self.style.theme_use('alt')

        self.gui_program()
        self.gui_split_h()
        self.gui_message()

        self.grid(sticky='NSWE')
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)
        self.master.rowconfigure(0, weight=1)
        self.master.columnconfigure(0, weight=1)

    def gui_program(self):
        ''' 界面: 报警程序 '''
        self.frame_program = frame = ttk.Frame(self)
        self.y_tree_program = y_tp = ttk.Scrollbar(frame, orient=tk.VERTICAL)
        self.tree_program = tp = ttk.Treeview(
                frame, yscrollcommand=y_tp.set, columns='val', 
                )
        y_tp.configure(command=tp.yview)
        self.control_program = frame_cp = ttk.Frame(frame)
        self.flag_run = tk.BooleanVar(frame_cp, value=False)
        self.btn_set = btn_set = ttk.Button(frame_cp, text='参数设置')
        self.raido_working = radio_working = ttk.Radiobutton(
                frame_cp, text='运行', variable=self.flag_run, value=True,
                command=self.run_select,
                )
        self.raido_rest = radio_rest = ttk.Radiobutton(
                frame_cp, text='停止', variable=self.flag_run, value=False,
                command=self.run_select,
                )
        self.btn_stop_play = ttk.Button(
                frame_cp, text='停止音乐', state=tk.DISABLED,
                )
        btn_set.grid()
        radio_working.grid()
        radio_rest.grid()
        self.btn_stop_play.grid()
        y_tp.grid(row=0, column=0, pady=5, sticky='NS')
        tp.grid(row=0, column=1, pady=5, stick='NSWE')
        frame_cp.grid(row=0, column=2, sticky='NS')
        frame.columnconfigure(1, weight=1)
        frame.grid(padx=5, stick='NSWE')

    def gui_split_h(self):
        ''' 界面: 分割 '''
        self.MidSeparator = ms = ttk.Separator(self, orient=tk.HORIZONTAL)
        ms.grid(sticky='WE', padx=5, pady=5)

    def gui_message(self):
        ''' 界面: 报警信息 '''
        arr_text = (
                ('#0', '报警时间'),
                ('stock_code', '股票代码'),
                ('period', 'k线周期'),
                ('message', '信息'),
                )
        self.frame_message = frame = ttk.Frame(self)
        self.y_table_message = y_tm= ttk.Scrollbar(frame, orient=tk.VERTICAL)
        self.table_message = tm = ttk.Treeview(
                frame, yscrollcommand=y_tm.set,
                columns=('stock_code', 'period', 'message'),
                )
        y_tm.configure(command=tm.yview)
        for k, v in arr_text:
            tm.heading(k, text=v, anchor='w')
            tm.column(k, anchor='w')
        y_tm.grid(row=0, column=0, sticky='NS')
        tm.grid(row=0, column=1, sticky='NSWE')
        frame.rowconfigure(0, weight=1)
        frame.columnconfigure(1, weight=1)
        frame.grid(padx=5, stick='NSWE')
 
    def load_data(self):
        ''' 读取数据表
            only_today          仅显示今天的报警信息
        '''
        info_period = {
                '1m': '1分钟',
                '5m': '5分钟',
                }
        # 报警程序
        self.load_alarm_program()
        # 报警信息
        self.load_alarm_message(False)

    def load_alarm_program(self):
        ''' 读取数据表: 报警程序 '''
        tp = self.tree_program
        for _, obj_program in self.obj_KlineInfo.info_program.items():
            info = obj_program.info_program
            # 文件名
            program_name = tp.insert(
                    '', 'end', text=info['algorithm'], values=info['remark'],
                    )
            # 监控的股票
            stock_info = tp.insert(program_name, 'end', text='股票信息')
            for stock_code in info['arr_stock_code']:
                stock_code_zh = self.df_stock_code.loc[stock_code].display_name
                tp.insert(
                        stock_info, 'end', text=stock_code,
                        values=stock_code_zh,
                        )
            # k线周期
            period_info = tp.insert(program_name, 'end', text='k线周期')
            for period in info['arr_period']:
                tp.insert(period_info, 'end', text=period, values=[period])
            # 附加信息
            other_info = tp.insert(program_name, 'end', text='附加信息')
            if info['other_kwargs']:
                for key, value in info['other_kwargs'].items():
                    tp.insert(other_info, 'end', text=key, values=value)

    def load_alarm_message(self, only_today=True):
        ''' 读取数据表: 报警信息 '''
        tm = self.table_message
        today = datetime.date.today()
        df = self.obj_KlineInfo.obj_DataTable.read_db__alarm_message()
        df.sort_index(inplace=True)
        for (s_now, stock_code, period), v in df.iterrows():
            message = v.message
            if only_today and pd.Timestamp(s_now).date() != today:
                # 仅显示今天的报警信息
                continue
            stock_code_zh = self.df_stock_code.loc[stock_code].display_name
            tm.insert(
                    '', 0, text=s_now, values=(stock_code_zh, period, message),
                    )

    def update_alarm_message(self):
        ''' 刷新"报警信息"窗口的内容 '''
        tm = self.table_message
        tm.delete(*tm.get_children())
        self.load_alarm_message()

    def run_select(self):
        s_now = datetime.datetime.now().isoformat()
        if self.flag_run.get():
            print(f'报警程序开始运行 ... {s_now}')
            self.update_clock()
            # 每次点击"运行"时，都需要刷新"报警信息"窗口的内容
            self.only_once = True
        else:
            print(f'报警程序停止运行 ... {s_now}')
            self.master.after_cancel(self.id_after)
            self.id_after = None
            self.only_once = False

    def init_alarm_program(self):
        ''' 初始报警程序 '''
        self.only_once = True
        self.obj_KlineInfo = a_s.KlineInfo()
        self.df_stock_code = self.obj_KlineInfo.obj_DataTable.read_db__stock_code()

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

    def update_clock(self):
        ''' 更新定时器 '''
        self.job()
        now = datetime.datetime.now()
        delta_seconds, delta_microseconds = self.calc_delta_time(now)
        delta_time = int(delta_seconds * 1000 + delta_microseconds / 1000.0)
        self.id_after = self.master.after(delta_time, self.update_clock)

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
                self.update_alarm_message()

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
                next_time = t_am_begin + wait_day
        else:
            # 周末
            wait_day = datetime.timedelta(days=7 - now_weekday)
            next_time = t_am_begin + wait_day
        delta_time = next_time - now
        return (delta_time.seconds, delta_time.microseconds)


def main():
    root = tk.Tk()
    app = Application(root)
    root.mainloop()


if __name__ == '__main__':
    main()

