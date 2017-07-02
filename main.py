import authorize
import re
import sqlite3
import time
import progressbar
import vk.api

del_trash = re.compile(r"[\'\"]+|\\$]", re.U)


def dump_message_pack(dialog_id, messages, cursor, regexp=del_trash):
    for msg in messages['items']:
        if msg["body"] == "":
            continue
        cursor.execute(r"""INSERT OR REPLACE INTO t%s VALUES (%s, "%s", %s)"""
                       % (dialog_id, msg["id"],
                          regexp.sub("", msg["body"]), msg["date"]))
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
        cursor.execute("DROP TABLE dialogs")
        dialogs = vk.api.get_all_dialogs(token)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dialogs (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, name TEXT)")
        for info in dialogs:
            dialog = info["message"]
            dialog_id = vk.api.get_id_of_dialog(dialog)
            cursor.execute("SELECT * FROM dialogs WHERE dialog_id=%s" % dialog_id)
            if cursor.fetchone() is not None:
                continue
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog_id)
            name = vk.api.get_name_of_dialog(dialog_id, dialog["title"], token)
            cursor.execute("INSERT OR REPLACE INTO dialogs VALUES (?, ?)",
                           (dialog_id, name))
            first_response = vk.api.collect_messages(dialog_id, 0, token)
            messages_in_dialog = first_response['count']
            bar = progressbar.ProgressBar(max_value=messages_in_dialog,
                                          widgets=[
                                              progressbar.Percentage(), " ",
                                              progressbar.SimpleProgress(),
                                              ' [', progressbar.Timer(), '] ',
                                              progressbar.Bar(), name,
                                          ])
            dump_message_pack(dialog_id, first_response, cursor)
            offset = 4000
            bar.update(min(offset, messages_in_dialog))
            fixed_offset = 4000
            while offset < messages_in_dialog:
                response = vk.api.collect_messages(dialog_id, offset, token)
                dump_message_pack(dialog_id, response, cursor)
                offset = offset + fixed_offset
                bar.update(min(offset, messages_in_dialog))
            print(" " + name + "\'s dialog dumped\n")
            db.commit()


if __name__ == "__main__":
    main()
