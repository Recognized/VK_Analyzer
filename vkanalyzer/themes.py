import codecs
import json
import os
import urllib.request, urllib.parse
import pymorphy2
import pickle
import time
import re


class Theme:
    missed_words = dict()
    morph = pymorphy2.MorphAnalyzer()

    @staticmethod
    def normalize_word(word):
        return Theme.morph.parse(word)[0].normal_form

    def _find_related(self, word, depth, value):
        """Finds most similar words to this one from selerex database for russian tesaurus"""

        # not so fast, please
        word = word.lower().strip()
        time.sleep(0.2)
        link = "http://www.serelex.org/find/ru-skipgram-librusec/" + urllib.parse.quote_plus(word)
        print(link)
        page = urllib.request.urlopen(link).read()
        response = json.loads(page)
        try:
            if response["totalRelations"] == 0:
                print(word)
                return
            response = response["relations"]
        except KeyError:
            Theme.missed_words.setdefault(word, (depth, value))
            return
        for obj in response:
            x = Theme.normalize_word(obj["word"])
            self.keywords.setdefault(x, value * obj["value"])
            if depth != 0:
                self._find_related(x, depth - 1, value * obj["value"])

    def __init__(self, start_keywords):
        self.name = start_keywords[0]
        self.keywords = {Theme.normalize_word(k): 1.0 for k in start_keywords}
        for k_1, v_1 in self.keywords.copy().items():
            self._find_related(k_1, depth=1, value=1)
        # while Theme.missed_words != dict():
        #     print("Not all words were collected: ")
        #     print(Theme.missed_words)
        #     for k, v in Theme.missed_words.copy().items():
        #         self._find_related(k, v[0], v[1])


def initialize_themes(filename):
    with codecs.open(filename, "r", encoding="utf-8") as file:
        for line in file:
            t = Theme(line.split())
            with codecs.open("themes/"+t.name+".pckl", "wb") as dump:
                pickle.dump(t.keywords, dump)


def get_themes():
    files = os.listdir("./themes")
    return {re.search("([а-яА-Я]+)", file).group(1): pickle.load(codecs.open("themes/" + file, "rb")) for file in files}

if __name__ == '__main__':
    initialize_themes("themelist.txt")