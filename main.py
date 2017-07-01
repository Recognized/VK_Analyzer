import authorize
import json
import urllib.parse, urllib.request
import re


def merge_list(a, b):
    for i in b:
        a.append(i)


def call_api(method, params, token):
    params["access_token"] = token
    url = "https://api.vk.com/method/%s?%s" % (method, urllib.parse.urlencode(params))
    print(url)
    info = urllib.request.urlopen(url).read()
    try:
        return json.loads(info)["response"]
    except:
        print(info)
        raise Exception("Wrong response received")


def getAllDialogs(token):
    fixed_offset = 200
    first_response = call_api("messages.getDialogs", {"offset": "0", "count": "200", "v": "5.65"}, token)
    dialogs_left = max(int(first_response['count']) - fixed_offset, 0)
    dialogs_list = first_response['items']
    i = 1
    while (dialogs_left > 0):
        response = call_api("messages.getDialogs", {"offset": i * fixed_offset, "count": "200", "v": "5.65"}, token)
        merge_list(dialogs_list, response['items'])
        dialogs_left = dialogs_left - fixed_offset
    return dialogs_list


client_id = "6096377"
scope = "messages,offline"

token = ""
user_id = ""

try:
    with open("token", "r") as file:
        token, user_id = (word for line in file for word in re.findall(r'\w+', line))
except:
    with open("token", "w") as file:
        email = str(input("Email: "))
        password = str(input("Password: "))
        token, user_id = authorize.auth(email, password, client_id, scope)
        file.write(token + "\n" + user_id)

print("Authorization succeded\n")

dialogs = getAllDialogs(token)

for i in dialogs:
    print(i['message']['body'], '\n')
