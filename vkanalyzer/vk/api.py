import json
import urllib.parse, urllib.request


def merge_list(a, b):
    for i in b:
        a.append(i)


# this function tooks major time
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


def collect_messages(dialog, start, token):
    return call_api("execute.getMessages", peer_id=dialog.msgid, start_message_id=start, access_token=token)


def get_first_message_id(dialog, token):
    return call_api("messages.getHistory", peer_id=dialog.msgid, access_token=token, rev=1)["items"][0]["id"]


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


class Dialog:
    def __init__(self, info, token):
        # case multichat
        self.token = token
        if "chat_id" in info:
            self.name = info["title"]
            self.id = info["chat_id"] + 2000000000
            self.type = "chat"
            self.msgid = self.id
        # case user
        elif info["user_id"] > 0:
            self.id = info["user_id"]
            ans = call_api("users.get", user_id=self.id, access_token=token)
            self.name = ans[0]["first_name"] + " " + ans[0]["last_name"]
            self.type = "user"
            self.msgid = self.id
        # case community chat
        else:
            self.id = -info["user_id"]
            self.msgid = -self.id
            ans = call_api("groups.getById", group_id=self.id, access_token=token)
            self.name = ans[0]["name"]
            self.type = "community"




