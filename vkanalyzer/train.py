import pymorphy2
import sqlite3
import re
import codecs
from gensim.models.word2vec import Word2Vec, LineSentence
import logging
import vkanalyzer.forks.progressbar_fork as progressbar
import multiprocessing as mp

regexp = re.compile("[\\w]+")

count_of_sentences = 0

class Conversation:

    time_out = 35 * 60

    def __init__(self, messages_rows):
        self.begin = messages_rows[0][2]
        self.end = messages_rows[-1][2]  # sqlite: (message_id, body, date)
        self.size = len(messages_rows)
        self.text = ""
        for message in messages_rows:
            body = message[1]
            # TODO: smth was here

    def __len__(self):
        return self.size


def sentence(temp, file):
    if len(temp) == 0:
        return
    b = False
    for y in temp:
        if y != "":
            b = True
            file.write(y)
            if y != temp[-1]:
                file.write(' ')
    if b:
        global count_of_sentences
        count_of_sentences += 1
        file.write(u"\u000A")


def get_all_conversations():
    with sqlite3.connect("dialogs.sqlite") as db, codecs.open("RAW_DATA.txt", "w", encoding="UTF-8") as output:
        cursor = db.cursor()
        cursor.execute("SELECT dialog_id FROM dialogs")
        copy = cursor.fetchall()
        for i, dialog_id_row in enumerate(copy):
            dialog_id = dialog_id_row[0]
            try:
                cursor.execute("SELECT * FROM norm_t%s" % dialog_id)
            except:
                pass
            rows = cursor.fetchall()
            breakpoints = []
            for i in range(len(rows)-1):
                if rows[i + 1][2] - rows[i][2] > Conversation.time_out:
                    breakpoints.append(i)
            breakpoints.append(len(rows)-1)
            temp = []
            j = 0
            for i in range(len(rows)):
                temp.append(rows[i][1])
                if i == breakpoints[j]:
                    sentence(temp, output)
                    temp = []
                    j += 1


def start_training():
    get_all_conversations()
    print(count_of_sentences)
    logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    logging.root.setLevel(level=logging.INFO)
    model = Word2Vec(iter=1, min_count=3, window=5, size=200, workers=mp.cpu_count())
    model.build_vocab(LineSentence("RAW_DATA.txt"))
    with codecs.open("RAW_DATA.txt", "r", encoding="utf-8") as file:
        model.train([line.split() for line in file], total_examples=count_of_sentences, epochs=10)
        print(model.train_count)
    model.save("model1.model")






