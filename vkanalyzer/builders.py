import operator
import re
import time
import sqlite3
from vkanalyzer.vk import api
from vkanalyzer.themes import get_themes
import progressbar
import pymorphy2
import multiprocessing as mp
import os
from itertools import *
from pprint import pprint

del_trash = re.compile(r"[\'\"]+|\\$]", re.U)
other_symbols = re.compile(r"[^а-яА-Я ]", re.U)
processors = os.cpu_count()


def normalize_message(dump):
    """This function transorms each word to its normal form.
       For example: known -> know"""
    morph = pymorphy2.MorphAnalyzer()
    ans = []
    for k in dump:
        body = k[1]
        text = ""
        body = other_symbols.sub("", body)
        for word in re.split("[\\W]+", body):
            m = morph.parse(word)
            if len(m) > 0:
                wrd = m[0]
                if wrd.tag.POS not in ('NUMR', 'NPRO', 'PREP', 'CONJ', 'PRCL', 'INTJ'):
                    text += wrd.normal_form + " "
        ans.append((k[0], text.strip(), k[2]))
    return ans


def dump_message_pack(dialog_id : int, ans, cursor, regexp=del_trash):
    ms = lambda: int(round(time.time() * 1000))
    start = ms()
    dump = [(msg["id"], regexp.sub("", msg["body"]), msg["date"])
            for messages in ans for msg in reversed(messages["items"])
            if msg["body"] != ""]
    cursor.executemany("INSERT OR REPLACE INTO t%s VALUES (?, ?, ?)" % dialog_id, dump)
    cursor.executemany("INSERT OR REPLACE INTO norm_t%s VALUES (?, ?, ?)" % dialog_id, normalize_message(dump))
    end = ms()
    time.sleep(max(0, 1 - (end-start)))


def create_or_complete_database(token):
    with sqlite3.connect("dialogs.sqlite") as db:
        cursor = db.cursor()
        dialogs = api.get_all_dialogs(token)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dialogs (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, name TEXT)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS last_message_id (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, message_id INT)")
        for info in dialogs:
            dialog = api.Dialog(info["message"], token)

            cursor.execute("SELECT message_id FROM last_message_id WHERE dialog_id=%s" % dialog.id)
            start_message_id = cursor.fetchone()
            if start_message_id is None:
                start_message_id = api.get_first_message_id(dialog, token)
            else:
                start_message_id = start_message_id[0]
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog.id)
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS norm_t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog.id)
            response = api.collect_messages(dialog, start_message_id, token)
            if response["result"][0]["items"][0]["id"] == start_message_id:
                print(" " + dialog.name + "\'s dialog is already dumped\n")
                time.sleep(1)
                continue
            cursor.execute("INSERT OR REPLACE INTO dialogs VALUES (?, ?)", (dialog.id, dialog.name))

            if "skipped" in response["result"][0]:
                bar_index = response["result"][0]["skipped"]
            else:
                j = 0
                for i in response["result"][0]["items"]:
                    if i["id"] > start_message_id:
                        j = j + 1
                bar_index = j

            start_message_id = response["new_start"]

            bar = progressbar.ProgressBar(max_value=bar_index,
                                          widgets=[
                                              progressbar.Percentage(), " ",
                                              progressbar.SimpleProgress(),
                                              ' [', progressbar.Timer(), '] ',
                                              progressbar.Bar(), dialog.name,
                                          ])
            dump_message_pack(dialog.id, response["result"], cursor)

            bar.update(0)

            while "skipped" in response["result"][0]:
                bar.update(bar_index - response["result"][0]["skipped"])
                response = api.collect_messages(dialog, start_message_id, token)
                start_message_id = response["new_start"]

                dump_message_pack(dialog.id, response["result"], cursor)
            bar.finish()
            cursor.execute("INSERT OR REPLACE INTO last_message_id VALUES (?, ?)", (dialog.id, start_message_id))
            db.commit()


def build_word_frequencies_table():
    all_words_freq = dict()
    with sqlite3.connect("frequencies.sqlite") as table, sqlite3.connect("dialogs.sqlite") as db:
        freqs = table.cursor()
        dialogs = db.cursor()
        dialogs.execute("SELECT dialog_id FROM dialogs")
        copy = dialogs.fetchall()

        bar = progressbar.ProgressBar(max_value=len(copy) - 1,
                                      widgets=[
                                          progressbar.Percentage(), " ",
                                          progressbar.SimpleProgress(),
                                          ' [', progressbar.Timer(), '] ',
                                          progressbar.Bar(), "Frequencies are counting",
                                      ])

        for row, i in zip(copy, range(len(copy))):
            bar.update(i)
            dialog_id = row[0]
            dialog_freq = dict()
            dialogs.execute("SELECT body FROM norm_t%s" % dialog_id)
            for message_row in dialogs.fetchall():
                body = message_row[0]
                for word in re.split('[., ]+', body):
                    s = word.lower()
                    all_words_freq.setdefault(s, 0)
                    dialog_freq.setdefault(s, 0)
                    all_words_freq[s] += 1
                    dialog_freq[s] += 1
            freqs.execute("CREATE TABLE IF NOT EXISTS t%s (word TEXT, counter INT)" % dialog_id)
            freqs.executemany("INSERT OR REPLACE INTO t%s VALUES (?, ?)" % dialog_id,
                              [(i, j) for i, j in dialog_freq.items()])
        freqs.execute("CREATE TABLE IF NOT EXISTS global (word TEXT, counter INT)")
        freqs.executemany("INSERT OR REPLACE INTO global VALUES(?, ?)",
                          [(i, j) for i, j in all_words_freq.items()])
        table.commit()

        bar.finish()


def build_stat_by_time(parse_time, table_name):
    """:param parse_time : it is a function that determines period of time (like every day or week) which
                          this exact millisecond of system time belong to
                          example: lambda x: x // (7 * 24 * 60 * 60), "week" """

    with sqlite3.connect("dialogs.sqlite") as db, sqlite3.connect("%s.sqlite" % table_name) as table:
        dialogs = db.cursor()
        activity = table.cursor()

        stat_all = dict()
        gtime_all = dict()
        dialogs.execute("SELECT dialog_id FROM dialogs")
        copy = dialogs.fetchall()

        bar = progressbar.ProgressBar(max_value=len(copy) - 1,
                                      widgets=[
                                          progressbar.Percentage(), " ",
                                          progressbar.SimpleProgress(),
                                          ' [', progressbar.Timer(), '] ',
                                          progressbar.Bar(), "Activity is counting",
                                      ])

        for row, i in zip(copy, range(len(copy))):
            bar.update(i)
            dialog_id = row[0]
            stat_cur = dict()
            gtime_cur = dict()
            dialogs.execute("SELECT date FROM t%s" % dialog_id)
            for message_row in dialogs.fetchall():
                t = message_row[0]
                when = parse_time(t)
                stat_all.setdefault(when, 0)
                stat_cur.setdefault(when, 0)
                gtime_all.setdefault(when, t)
                gtime_cur.setdefault(when, t)
                stat_all[when] += 1
                stat_cur[when] += 1

            activity.execute("CREATE TABLE IF NOT EXISTS t%s (time INT, counter INT, gtime INT)" % dialog_id)
            activity.executemany("INSERT OR REPLACE INTO t%s VALUES (?, ?, ?)" % dialog_id,
                                 [(i, j, gtime_cur[i]) for i, j in stat_cur.items()])
        activity.execute("CREATE TABLE IF NOT EXISTS global (time INT, counter INT, gtime INT)")
        activity.executemany("INSERT OR REPLACE INTO global VALUES(?, ?, ?)",
                             [(i, j, gtime_all[i]) for i, j in stat_all.items()])
        table.commit()

        bar.finish()


def build_stat_by_daytime():
    regexp = re.compile("\\W+")
    build_stat_by_time(lambda x: int(regexp.split(time.ctime(x))[3][:2]), "daytime")


def build_stat_by_week():
    build_stat_by_time(lambda x: x // (7 * 24 * 60 * 60), "week")


def build_stat_by_day():
    build_stat_by_time(lambda x: x // (24 * 60 * 60), "day")

# -------------------------------------------------
# New functionality


def build_themes(dialog_id, themes):
    score = dict()
    with sqlite3.connect("dialogs.sqlite") as db:
        dialogs = db.cursor()
        dialogs.execute("SELECT body FROM norm_t%s" % dialog_id)
        copied = dialogs.fetchall()
        dialogs.execute("CREATE TABLE IF NOT EXISTS lengths (dialog_id INTEGER PRIMARY KEY, length INT)")
        dialogs.execute("INSERT OR REPLACE INTO lengths VALUES (?, ?)", (dialog_id, len(copied)))
        for row in copied:
            body = row[0]
            for word in body.split():
                for name, theme in themes.items():
                    if word in theme:
                        score.setdefault(name, 0)
                        score[name] += theme[word] ** 3
    ac = sum([v for k, v in score.items()])
    out = {k: v/ac*100 for k, v in score.items()}
    with sqlite3.connect("themes.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS t%s (theme TEXT, score DOUBLE)" % dialog_id)
        cursor.executemany("INSERT OR REPLACE INTO t%s VALUES (?, ?)" % dialog_id, out.items())


def build_themes_relatively():
    threshold = 2000 # messages
    themes = get_themes()
    with sqlite3.connect("dialogs.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM dialogs")
        dialogs = [row[0] for row in cursor.fetchall()]
    for dialog in dialogs:
        build_themes(dialog, themes)
    with sqlite3.connect("dialogs.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("SELECT * FROM lengths")
        lengths = cursor.fetchall()
    lengths = list(filter(lambda x: x[1] > threshold, lengths))
    with sqlite3.connect("themes.sqlite") as table:
        cursor = table.cursor()
        score_min, score_max = {}, {}
        for id, length in lengths:
            cursor.execute("SELECT * FROM t%s" % id)
            for row in cursor.fetchall():
                print(row[0], " ", row[1])
                score_min.setdefault(row[0], row[1])
                score_max.setdefault(row[0], row[1])
                score_min[row[0]] = min(score_min[row[0]], row[1])
                score_max[row[0]] = max(score_max[row[0]], row[1])
        for id, length in lengths:
            print(id)
            cursor.execute("SELECT * FROM t%s" % id)
            result = {}
            for row in cursor.fetchall():
                diff = (score_max[row[0]] - score_min[row[0]])/2
                mid = score_min[row[0]] + diff
                result[row[0]] = (row[1] - mid)/diff*100
            cursor.execute("CREATE TABLE IF NOT EXISTS rel_t%s (name TEXT, score DOUBLE)" % id)
            cursor.executemany("INSERT OR REPLACE INTO rel_t%s VALUES(?, ?)" % id, result.items())


if __name__ == '__main__':
    build_themes_relatively()
    dialog_id = 180207650
    with sqlite3.connect("themes.sqlite") as table:
        cursor = table.cursor()
        cursor.execute("SELECT * FROM rel_t%s" % dialog_id)
        ans = cursor.fetchall()
        pprint(sorted(ans, key=operator.itemgetter(1), reverse=True))