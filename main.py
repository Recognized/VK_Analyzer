import authorize
import getpass

client_id = "6096377"
scope = "messages,offline"

email = input("Email: ")
password = getpass.getpass()

token, user_id = authorize.auth(email, password, client_id, scope)