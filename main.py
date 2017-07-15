import vkanalyzer.vk.authorize
from vkanalyzer.builders import *
from vkanalyzer.drawers import *


def main():
    # TODO: use instruments from here
    token, user_id = vkanalyzer.vk.authorize.authorization()
    create_or_complete_database(token)
    # build_word_frequencies_table()
    # build_stat_by_week()
    # build_stat_by_day()

    day_plot(1)
    day_accum_plot(1)


if __name__ == '__main__':
    main()