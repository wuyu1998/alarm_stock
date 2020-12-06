# -*- encoding: utf-8 -*-
''' Tkinter图形界面
'''

import datetime
import tkinter as tk
import tkinter.ttk as ttk
import vlc

import alarm_stock as a_s
import settings as settings

class Application(ttk.Frame):
    # 闭市后，监控程序继续运行的时间
    t_continue_run = datetime.timedelta(seconds=settings.n_continue_run)

    def __init__(self, master=None):
        super().__init__(master)
        self.init_gui()
        self.update_clock()

    def init_gui(self):
        pass

    def update_clock(self):
        now = datetime.datetime.now()
        delta_seconds, delta_microseconds = self.calc_delta_time(now)
        x_after = self.root.after(
                int(delta_seconds * 1000 + delta_microseconds / 1000.0),
                self.update_clock
                )

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




root = tk.Tk()
app = App(root)
root.mainloop()
