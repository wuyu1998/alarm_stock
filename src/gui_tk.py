# -*- encoding: utf-8 -*-
''' Tkinter图形界面
'''

import datetime
import tkinter as tk
import tkinter.ttk as ttk

import alarm_stock as a_s

class Application(ttk.Frame):
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
        x = now + datetime.timedelta(minutes=self.delta_time)
        next_time = datetime.datetime(
                x.year, x.month, x.day, x.hour, x.minute, 3
                )
        delta_time = next_time - now
        return (delta_time.seconds, delta_time.microseconds)


root = tk.Tk()
app = App(root)
root.mainloop()
