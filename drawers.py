from pandas import DataFrame, date_range
import sqlite3
import time
import os
import matplotlib.pyplot as plt

accum = 0


def identity(v):
    return v


def prefix_sum(v):
    global accum
    accum += v
    return accum


def save(name='', fmt='png'):
    pwd = os.getcwd()
    path = '/pictures/{}'.format(fmt)
    if not os.path.exists(path):
        os.mkdir(path)
    os.chdir(path)
    plt.savefig('{}.{}'.format(name, fmt), fmt='png', dpi=400)
    os.chdir(pwd)


def period_plot(dialog_id, tableName, freq, transform):
    with sqlite3.connect("%s.sqlite" % tableName) as table:
        week = table.cursor()
        week.execute("SELECT * FROM t%s ORDER BY time ASC" % dialog_id)
        res = week.fetchall()
        begin, end = res[0][2], res[-1][2]
        first, last = res[0][0], res[-1][0]
        data = dict()
        for i in range(last - first + 1):
            data.setdefault(i, 0)
        for row in res:
            data[row[0] - first] = row[1]
        for k, v in data.copy().items():
            data[k] = transform(v)
        df = DataFrame(data=[v for k, v in data.items()],
                       index=date_range(start=time.strftime("%m/%d/%Y", time.gmtime(begin)), periods=len(data),
                                        freq=freq),
                       columns=[dialog_id])
        df.plot()
        save("%s_%s_%s_%s" % (dialog_id, tableName, freq, time.time()))


def week_plot(dialog_id):
    period_plot(dialog_id, "week", "7D", identity)


def day_plot(dialog_id):
    period_plot(dialog_id, "day", "D", identity)


def day_accum_plot(dialog_id):
    period_plot(dialog_id, "day", "D", prefix_sum)



