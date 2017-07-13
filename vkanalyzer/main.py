from vkanalyzer.builders import *
from vkanalyzer.drawers import *
from vkanalyzer.vk import authorize
import vkanalyzer.themes


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


def main():
    # token, user_id = authorization()
    # create_or_complete_database(token)
    # build_word_frequencies_table()
    # build_stat_by_week()
    # build_stat_by_day()
    # day_plot(10278660)
    # day_accum_plot(10278660)
    # start_training()
    vkanalyzer.themes.initialize_themes("themelist.txt")



if __name__ == "__main__":
    main()
