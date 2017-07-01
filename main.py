import authorize
import json
import urllib.parse, urllib.request
import re
import sqlite3
import codecs


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
    url = "https://api.vk.com/method/%s?%s" % (method, urllib.parse.urlencode(params))
    return read_response(url)


def collect_messages(token, peer_id):
    params = {"peer_id": peer_id, "offset": 0}
    return call_api("execute.getMessages", params, token)


def get_all_dialogs(token):
    fixed_offset = 200
    first_response = call_api("messages.getDialogs", {"offset": "0", "count": "200", "v": "5.65"}, token)
    dialogs_left = max(int(first_response['count']) - fixed_offset, 0)
    dialogs_list = first_response['items']
    i = 1
    while dialogs_left > 0:
        response = call_api("messages.getDialogs", {"offset": i * fixed_offset, "count": "200", "v": "5.65"}, token)
        merge_list(dialogs_list, response['items'])
        dialogs_left = dialogs_left - fixed_offset
    return dialogs_list


def get_name_of_dialog(user_id, title, token):
    if title != " ... ":
        return title
    else:
        ans = call_api("users.get", {"user_id": user_id, "v": "5.65"}, token)
        return ans[0]["first_name"] + ans[0]["last_name"]


def table_name_by_id(id):
    return "t" + str(id)


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
    with codecs.open("debug.txt", "w", encoding="utf-8") as debug:
        with sqlite3.connect("dialogs.sqlite") as db:
            cursor = db.cursor()
            dialogs = get_all_dialogs(token)
            del_trash = re.compile(r"[\'\"]+|\\$", re.U)
            cursor.execute("CREATE TABLE IF NOT EXISTS dialogs (dialog_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, name TEXT)")
            for info in dialogs:
                dialog = info["message"]
                # cursor.execute("SELECT * FROM dialogs WHERE dialog_id=%s" % str(dialog["id"]))
                # if cursor.fetchone() is None:
                cursor.execute("CREATE TABLE IF NOT EXISTS %s (message_id INTEGER PRIMARY KEY ON CONFLICT REPLACE, body TEXT, date INT)"
                               % table_name_by_id(dialog["user_id"]))
                cursor.execute("INSERT INTO dialogs VALUES (?, ?)",
                               (dialog["id"], get_name_of_dialog(dialog["user_id"], dialog["title"], token)))
                messages = collect_messages(token, dialog["user_id"])
                for array in messages['result']:
                    for msg in array['items']:
                        if msg["body"] == "":
                            continue
                        cursor.execute(r"""INSERT INTO %s VALUES (%s, "%s", %s)"""
                                       % (table_name_by_id(dialog["user_id"]), msg["id"],
                                          del_trash.sub("", msg["body"]), msg["date"]))
                        debug.write(del_trash.sub(" ", msg["body"])+"\n")
            # msg["body"].replace("\"", "").replace("\'", "")

if __name__ == "__main__":
    main()
