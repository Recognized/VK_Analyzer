import sqlite3
import codecs
from gensim.models.word2vec import Word2Vec, LineSentence
from sklearn.cluster import KMeans, MeanShift
import numpy as np
import logging
import multiprocessing as mp

count_of_sentences = 0
time_out = 35 * 60


class Vocab:
    def __init__(self, filename, model, invert_weights=lambda x: 1 / x):
        self._data = dict()
        self.all = 0
        temp = []
        with codecs.open(filename, "r", encoding="utf-8") as source:
            for line in source:
                for word in line.split():
                    self._data.setdefault(word, 0)
                    self._data[word] += 1
                    self.all += 1
        to_del = []
        for k, v in self._data.items():
            if v < model.min_count:
                to_del.append(k)
                continue
            self._data[k] = invert_weights(v)
            temp.append(model.wv[k])
        for k in to_del:
            del self._data[k]
        self.vectors = np.array(temp)

    def __getitem__(self, key: str):
        return self._data[key]

    def items(self):
        return self._data.items()


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
            for i in range(len(rows) - 1):
                if rows[i + 1][2] - rows[i][2] > time_out:
                    breakpoints.append(i)
            breakpoints.append(len(rows) - 1)
            temp = []
            j = 0
            for i in range(len(rows)):
                temp.append(rows[i][1])
                if i == breakpoints[j]:
                    sentence(temp, output)
                    temp = []
                    j += 1


def checkEqualIvo(lst):
    return not lst or lst.count(lst[0]) == len(lst)


def initialize_centers_ones_diagonal(data, k=None, random_state=None, eps=None):
    """Initialization only for number of clusters equal to n_features"""
    ans = []
    for i in range(data.shape[1]):
        temp = []
        for j in range(data.shape[1]):
            if i == j:
                temp.append(20)
            else:
                temp.append(1e-18)
        ans.append(temp)
    return None, np.array(ans)


def start_training():
    # get_all_conversations()
    # print(count_of_sentences)
    # logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
    # logging.root.setLevel(level=logging.INFO)
    # model = Word2Vec(iter=1, min_count=7, window=7, size=300, workers=mp.cpu_count())
    # model.build_vocab(LineSentence("RAW_DATA.txt"))
    # with codecs.open("RAW_DATA.txt", "r", encoding="utf-8") as file:
    #     model.train([line.split() for line in file], total_examples=count_of_sentences, epochs=15)
    #     print(model.train_count)
    # model.clear_sims()
    # model.save("model1.model")
    model = Word2Vec.load("model1.model")
    my_vocab = Vocab("RAW_DATA.txt", model)
    kmean = KMeans(n_clusters=40, random_state=19993101).fit(my_vocab.vectors)
    print(kmean.cluster_centers_)
    for vector in kmean.cluster_centers_:
        print(model.similar_by_vector(vector))
    print("---------------------------------------")
    del kmean
    meanshift = MeanShift().fit(my_vocab.vectors)
    print(meanshift.cluster_centers_)
    for vector in meanshift.cluster_centers_:
        print(model.similar_by_vector(vector))
    # lengths = [np.linalg.norm(x) for x in my_vocab.vectors]
    # print(lengths)
    # print(max(lengths))
    # print(min(lengths))
    # print(model.similar_by_vector(my_vocab.vectors[lengths.index(max(lengths))]))
    # print(model.similar_by_vector(my_vocab.vectors[lengths.index(min(lengths))]))
    # print(my_vocab.vectors.shape)
    # print(model["нд"])
    # print(model["квсполучить"])

