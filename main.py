import vkanalyzer.vk.authorize
from vkanalyzer.builders import *
from vkanalyzer.drawers import *
from pprint import pprint


def main():
    # TODO: use instruments from here
    token, user_id = vkanalyzer.vk.authorize.authorization()
    create_or_complete_database(token)
    # build_word_frequencies_table()
    # build_stat_by_week()
    # build_stat_by_day()
    dialog_id = 1 # TODO: your own dialog
    day_plot(dialog_id)
    day_accum_plot(dialog_id)
    
    build_themes_relatively()
    
    with sqlite3.connect("themes.sqlite") as table:
        cursor = table.cursor()
        cursor.execute("SELECT * FROM rel_t%s" % dialog_id)
        ans = cursor.fetchall()
        pprint(sorted(ans, key=operator.itemgetter(1), reverse=True))


if __name__ == '__main__':
    main()
