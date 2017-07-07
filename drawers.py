from pandas import DataFrame, date_range
import sqlite3
import time


def week_plot(dialog_id):
    with sqlite3.connect("week.sqlite") as table:
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
        df = DataFrame(data=[v for k, v in data.items()],
                       index=date_range(start=time.strftime("%m/%d/%Y", time.gmtime(begin)), periods=len(data),
                                        freq='7D'),
                       columns=[dialog_id])
        df.plot()


