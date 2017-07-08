import pymorphy2
import sqlite3
import re
from gensim.models.word2vec import Word2Vec
import vkanalyzer.forks.progressbar_fork as progressbar


class Conversation:

    time_out = 35 * 60

    def __init__(self, messages_rows):
        morph = pymorphy2.MorphAnalyzer()
        self.begin = messages_rows[0][2]
        self.end = messages_rows[-1][2]  # sqlite: (message_id, body, date)
        self.size = len(messages_rows)
        self.text = ""
        for message in messages_rows:
            body = message[1]
            #TODO: smth was here

    def __len__(self):
        return self.size


def get_all_conversations():
    with sqlite3.connect("dialogs.sqlite") as db:
        cursor = db.cursor()
        cursor.execute("SELECT dialog_id FROM dialogs")
        conversations = []

        copy = cursor.fetchall()

        cursor.execute("SELECT message_id FROM last_message_id ORDER BY message_id DESC")
        id = cursor.fetchall()[0][0]

        bar = progressbar.ProgressBar(max_value=id,
                                      widgets=[
                                          progressbar.Percentage(), " ",
                                          progressbar.SimpleProgress(),
                                          ' [', progressbar.Timer(), '] ',
                                          progressbar.Bar(), "Dialogs are transforming",
                                      ])

        for i, dialog_id_row in enumerate(copy):
            dialog_id = dialog_id_row[0]
            cursor.execute("SELECT * FROM t%s" % dialog_id)
            rows = cursor.fetchall()
            breakpoints = []
            for i in range(len(rows)-1):
                if rows[i + 1][2] - rows[i][2] > Conversation.time_out:
                    breakpoints.append(i)
            breakpoints.append(len(rows)-1)
            temp = []
            j = 0
            for i in range(len(rows)):
                temp.append(rows[i])
                bar.update(rows[i][0])
                if i == breakpoints[j]:
                    conv = Conversation(temp)
                    if conv.text != "":
                        conversations.append(conv)
                    temp = []
                    j += 1
        bar.finish()
        return conversations


def start_training():
    conversations = get_all_conversations()
    model = Word2Vec([text.text for text in conversations])
    model.save("model1.model")






