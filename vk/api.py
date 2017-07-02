import json
import urllib.parse, urllib.request


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


def call_api(method, **kwargs):
    kwargs["v"] = "5.65"
    url = "https://api.vk.com/method/%s?%s" % (method, urllib.parse.urlencode(kwargs))
    return read_response(url)


def collect_messages(peer_id, offset, token):
    return call_api("execute.getMessages", peer_id=peer_id, offset=offset, access_token=token)[0]


def get_all_dialogs(token):
    fixed_offset = 200
    first_response = call_api("messages.getDialogs", offset=0, count=200, access_token=token)
    dialogs_left = max(int(first_response['count']) - fixed_offset, 0)
    dialogs_list = first_response['items']
    i = 1
    while dialogs_left > 0:
        response = call_api("messages.getDialogs", offset=i * fixed_offset, count=200, access_token=token)
        merge_list(dialogs_list, response['items'])
        dialogs_left = dialogs_left - fixed_offset
    return dialogs_list


def get_id_of_dialog(dialog):
    if dialog["title"] != " ... ":
        return 2000000000 + dialog["chat_id"]
    else:
        return dialog["user_id"]


def get_name_of_dialog(user_id, title, token):
    if title != " ... ":
        return title
    else:
        ans = call_api("users.get", user_id=user_id, access_token=token)
        return ans[0]["first_name"] + " " + ans[0]["last_name"]
