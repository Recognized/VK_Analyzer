import authorize
import re
import sqlite3
import time
import forks.progressbar_fork as progressbar
import vk.api

del_trash = re.compile(r"[\'\"]+|\\$]", re.U)


def dump_message_pack(dialog_id, ans, cursor, regexp=del_trash):
    dump = [(msg["id"], regexp.sub("", msg["body"]), msg["date"])
            for messages in ans for msg in reversed(messages["items"])
            if msg["body"] != ""]
    cursor.executemany("INSERT OR REPLACE INTO t%s VALUES (?, ?, ?)" % dialog_id, dump)
    time.sleep(1)


def authorization():
    client_id = "6096377"
    scope = "messages,offline"

    try:
        with open("token", "r") as file:
            token, user_id = (word for line in file for word in re.findall(r'\w+', line))
    except FileNotFoundError:
        with open("token", "w") as file:
            email = str(input("Email: "))
            password = str(input("Password: "))
            token, user_id = authorize.auth(email, password, client_id, scope)
            file.write(token + "\n" + user_id)
    print("Authorization succeeded\n")
    return token, user_id


def create_or_complete_database(token):
    with sqlite3.connect("dialogs.sqlite") as db:
        cursor = db.cursor()
        dialogs = vk.api.get_all_dialogs(token)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dialogs (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, name TEXT)")
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS last_message_id (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, message_id INT)")
        for info in dialogs:
            dialog = vk.api.Dialog(info["message"], token)

            cursor.execute("SELECT message_id FROM last_message_id WHERE dialog_id=%s" % dialog.id)
            start_message_id = cursor.fetchone()
            if start_message_id is None:
                start_message_id = vk.api.get_first_message_id(dialog, token)
            else:
                start_message_id = start_message_id[0]
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog.id)
            cursor.execute("INSERT OR REPLACE INTO dialogs VALUES (?, ?)",
                           (dialog.id, dialog.name))
            response = vk.api.collect_messages(dialog, start_message_id, token)
            if response["result"][0]["items"][0]["id"] == start_message_id:
                print(" " + dialog.name + "\'s dialog is already dumped\n")
                time.sleep(1)
                continue

            if "skipped" in response["result"][0]:
                barIndex = response["result"][0]["skipped"]
            else:
                j = 0
                for i in response["result"][0]["items"]:
                    if i["id"] > start_message_id:
                        j = j + 1
                barIndex = j

            start_message_id = response["new_start"]

            bar = progressbar.ProgressBar(max_value=barIndex,
                                          widgets=[
                                              progressbar.Percentage(), " ",
                                              progressbar.SimpleProgress(),
                                              ' [', progressbar.Timer(), '] ',
                                              progressbar.Bar(), dialog.name,
                                          ])
            dump_message_pack(dialog.id, response["result"], cursor)

            bar.update(0)

            while "skipped" in response["result"][0]:
                bar.update(barIndex - response["result"][0]["skipped"])
                response = vk.api.collect_messages(dialog, start_message_id, token)
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

        bar = progressbar.ProgressBar(max_value=len(copy)-1,
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
            dialogs.execute("SELECT body FROM t%s" % dialog_id)
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


def main():
    token, user_id = authorization()
    # create_or_complete_database(token)
    build_word_frequencies_table()

if __name__ == "__main__":
    main()
