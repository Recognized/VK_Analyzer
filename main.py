import authorize
import re
import sqlite3
import time
import progressbar
import vk.api
import codecs

del_trash = re.compile(r"[\'\"]+|\\$]", re.U)


def dump_message_pack(dialog_id, ans, cursor, regexp=del_trash):
    dump = [(msg["id"], regexp.sub("", msg["body"]), msg["date"])
            for messages in ans for msg in reversed(messages["items"])
            if msg["body"] != ""]
    # for messages in ans:
    #     for msg in reversed(messages["items"]):
    #         if msg["body"] == "":
    #             continue
    #         dump.append((msg["id"], regexp.sub("", msg["body"]), msg["date"]))
    cursor.executemany(r"""INSERT OR REPLACE INTO t%s VALUES (?, ?, ?)""" % dialog_id, dump)
    time.sleep(1)


def main():
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


if __name__ == "__main__":
    main()
