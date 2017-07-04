import authorize
import re
import sqlite3
import time
import progressbar
import vk.api
import codecs

output = codecs.open("Arina.txt", "w", encoding="utf-8")
del_trash = re.compile(r"[\'\"]+|\\$]", re.U)


def dump_message_pack(dialog_id, messages, cursor, regexp=del_trash):
    for msg in messages["items"]:
        if msg["body"] == "":
            continue
        cursor.execute(r"""INSERT OR REPLACE INTO t%s VALUES (%s, "%s", %s)"""
                       % (dialog_id, msg["id"],
                          regexp.sub("", msg["body"]), msg["date"]))
        output.write(regexp.sub("", msg["body"])+'\n')
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
            "CREATE TABLE IF NOT EXISTS dialog_counter (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, counter INT)")
        for info in dialogs:
            dialog = vk.api.Dialog(info["message"], token)

            cursor.execute("SELECT counter FROM dialog_counter WHERE dialog_id=%s" % dialog.id)
            offset = cursor.fetchone()
            if offset is None:
                offset = 0
            else:
                offset = offset[0]
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog.id)
            cursor.execute("INSERT OR REPLACE INTO dialogs VALUES (?, ?)",
                           (dialog.id, dialog.name))
            offset = 0
            first_response = vk.api.collect_messages(dialog.id, offset, token)
            messages_in_dialog = first_response["count"]
            # if offset == messages_in_dialog:
            #     print(" " + dialog.name + "\'s dialog is already dumped\n")
            #     time.sleep(1)
            #     continue
            bar = progressbar.ProgressBar(max_value=messages_in_dialog,
                                          widgets=[
                                              progressbar.Percentage(), " ",
                                              progressbar.SimpleProgress(),
                                              ' [', progressbar.Timer(), '] ',
                                              progressbar.Bar(), dialog.name,
                                          ])
            dump_message_pack(dialog.id, first_response, cursor)
            fixed_offset = 4000
            offset = offset + fixed_offset
            bar.update(0)
            bar.update(min(offset, messages_in_dialog))
            while offset < messages_in_dialog:
                response = vk.api.collect_messages(dialog.id, offset, token)
                dump_message_pack(dialog.id, response, cursor)
                offset = offset + fixed_offset
                bar.update(min(offset, messages_in_dialog))
            cursor.execute("INSERT OR REPLACE INTO dialog_counter VALUES (?, ?)", (dialog.id, messages_in_dialog))
            print(" " + dialog.name + "\'s dialog dumped\n")
            output.close()
            db.commit()


if __name__ == "__main__":
    main()
