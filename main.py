import authorize
import json
import urllib.parse, urllib.request
import re
import sqlite3
import codecs
import time
import progressbar


def merge_list(a, b):
    for i in b:
        a.append(i)


def read_response(url):
    info = urllib.request.urlopen(url).read()
    try:
        return json.loads(info)["response"]
    except:
        print(info)
        raise Exception("Wrong response received")


def call_api(method, params, token):
    params["access_token"] = token
    params["v"] = "5.65"
    url = "https://api.vk.com/method/%s?%s" % (method, urllib.parse.urlencode(params))
    return read_response(url)


def collect_messages(token, peer_id, offset):
    params = {"peer_id": peer_id, "offset": offset}
    return call_api("execute.getMessages", params, token)[0]


def get_all_dialogs(token):
    fixed_offset = 200
    first_response = call_api("messages.getDialogs", {"offset": "0", "count": "200"}, token)
    dialogs_left = max(int(first_response['count']) - fixed_offset, 0)
    dialogs_list = first_response['items']
    i = 1
    while dialogs_left > 0:
        response = call_api("messages.getDialogs", {"offset": i * fixed_offset, "count": "200"}, token)
        merge_list(dialogs_list, response['items'])
        dialogs_left = dialogs_left - fixed_offset
    return dialogs_list


def get_name_of_dialog(user_id, title, token):
    if title != " ... ":
        return title
    else:
        ans = call_api("users.get", {"user_id": user_id}, token)
        return ans[0]["first_name"] + " " + ans[0]["last_name"]


def dump_message_pack(dialog_id, messages, cursor, regexp):
    for msg in messages['items']:
        if msg["body"] == "":
            continue
        try:
            cursor.execute(r"""INSERT OR REPLACE INTO t%s VALUES (%s, "%s", %s)"""
                       % (dialog_id, msg["id"],
                          regexp.sub("", msg["body"]), msg["date"]))
        except sqlite3.OperationalError:
            print(r"""INSERT OR REPLACE INTO t%s VALUES (%s, "%s", %s)"""
                       % (dialog_id, msg["id"],
                          regexp.sub("", msg["body"]), msg["date"]))
            raise Exception("Error in regexp")
    time.sleep(1)


def get_id_of_dialog(dialog):
    if dialog["title"] != " ... ":
        return 2000000000 + dialog["chat_id"]
    else:
        return dialog["user_id"]


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
        dialogs = get_all_dialogs(token)
        del_trash = re.compile(r"[\'\"]+|\\$]", re.U)
        cursor.execute(
            "CREATE TABLE IF NOT EXISTS dialogs (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, name TEXT)")
        for info in dialogs:
            dialog = info["message"]
            dialog_id = get_id_of_dialog(dialog)
            cursor.execute("SELECT * FROM dialogs WHERE dialog_id=%s" % dialog_id)
            if cursor.fetchone() is not None:
                continue
            cursor.execute(
                "CREATE TABLE IF NOT EXISTS t%s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                % dialog_id)
            name = get_name_of_dialog(dialog_id, dialog["title"], token)
            cursor.execute("INSERT OR REPLACE INTO dialogs VALUES (?, ?)",
                           (dialog_id, name))
            first_response = collect_messages(token, dialog_id, 0)
            messages_in_dialog = first_response['count']
            bar = progressbar.ProgressBar(max_value=messages_in_dialog,
                                          widgets=[
                                              progressbar.Percentage(), " ",
                                              progressbar.SimpleProgress(),
                                              ' [', progressbar.Timer(), '] ',
                                              progressbar.Bar(), name,
                                          ])
            dump_message_pack(dialog_id, first_response, cursor, del_trash)
            offset = 4000
            bar.update(min(offset, messages_in_dialog))
            fixed_offset = 4000
            while offset < messages_in_dialog:
                response = collect_messages(token, dialog_id, offset)
                dump_message_pack(dialog_id, response, cursor, del_trash)
                offset = offset + fixed_offset
                bar.update(min(offset, messages_in_dialog))
            print(" " + name + "\'s dialog dumped\n")
            db.commit()

if __name__ == "__main__":
    main()
