import requests
from bs4 import BeautifulSoup as bs
import nltk
from nltk.collocations import *
from nltk.stem.porter import *
from nltk.corpus import stopwords
from math import ceil
from flask import Flask

app = Flask(__name__)

@app.route('/')
def hello():
    return 'hello'

@app.route('/<int:i>')
def getQuestionStats(i):
    bigram_measures = nltk.collocations.BigramAssocMeasures()
    trigram_measures = nltk.collocations.TrigramAssocMeasures()

    # get the text and some features
    question_url = "http://www.willyoupressthebutton.com/{0}/".format(i)
    print(question_url)
    question_html = requests.get(question_url)
    question_bs = bs(question_html.text, 'html.parser')

    stemmer = PorterStemmer()
    stop = set(stopwords.words('english'))
    dilema_stems = [stemmer.stem(word) for word in question_bs.find(id="cond").text.strip().split() \
        if not word in stop]
    result_stems = [stemmer.stem(word) for word in question_bs.find(id="res").text.strip().split() \
        if not word in stop]
    total_stems = list(set(dilema_stems + result_stems))

    dilema_collocator = BigramCollocationFinder.from_words(dilema_stems)
    dilema_bigrams = dilema_collocator.nbest(bigram_measures.pmi, 10)

    res_collocator = BigramCollocationFinder.from_words(result_stems)
    res_bigrams = res_collocator.nbest(bigram_measures.pmi, 10)

    total_collocator = BigramCollocationFinder.from_words(total_stems)
    total_bigrams = total_collocator.nbest(bigram_measures.pmi, 10)


    # get answer statistics
    stats_html = requests.get("http://www.willyoupressthebutton.com/{0}/stats/yes".format(i))
    stats_bs = bs(stats_html.text, 'html.parser')
    yes_stats = stats_bs.find('span', {"class": "statsBarLeft"})
    yes_numbers = int(yes_stats.text.split()[0])
    yes_percentage = int(yes_stats.text.split()[1].strip("()%")) / 100.
    total_answers = ceil(yes_numbers / yes_percentage)

    return """
    total numbers: {0}
    total yeses: {1}
    dilema bigrams: {2}
    res bigrams: {3}
    total bigrams: {4}
    """.format( \
        total_answers, \
        yes_numbers, \
        dilema_bigrams, \
        res_bigrams, \
        total_bigrams)

if __name__ == "__main__":
    print("running server...")
    app.run(host='0.0.0.0')