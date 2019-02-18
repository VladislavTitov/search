import psycopg2
import uuid

# предполагается установленный модуль nltk
import nltk

# скачиваем нужные компоненты, при вызове этой команды откроется gui интерфейс, нужно выбрать popular (либо all) 
nltk.download()

from nltk.corpus import stopwords

# стеммер Портера предназначен только для английских слов, Snowball стеммер является развитием стеммера Портера
from nltk.stem.snowball import SnowballStemmer 

import re
import string

from pymystem3 import Mystem

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS words_porter(id uuid PRIMARY KEY, term varchar(64), article_id uuid)")
cur.execute("CREATE TABLE IF NOT EXISTS words_mystem(id uuid PRIMARY KEY, term varchar(64), article_id uuid)")

cur.execute("SELECT * FROM articles;")

words_porter = []
words_mystem = []

for item in cur:
    text = item[1] + " " + item[2] + " " + item[3]

    words = re.split(r'\W+', text)
    words = [w.lower() for w in words if w.isalpha()]

    stop_words = set(stopwords.words('russian'))
    words = [w for w in words if not w in stop_words]

    stemmer_eng = SnowballStemmer('english')
    stemmer_rus = SnowballStemmer('russian')
    stemmed = [stemmer_rus.stem(stemmer_eng.stem(word)) for word in words]

    for w in stemmed:
        words_porter.append((str(uuid.uuid4()), w, item[0]))
    
    stemmer_ya = Mystem(mystem_bin='./mystem')
    lemmas = stemmer_ya.lemmatize(' '.join(words))
    lemmas = [l for l in lemmas if len(l.strip()) > 0]

    for w in lemmas:
        words_mystem.append((str(uuid.uuid4()), w, item[0]))

    conn.commit()

for w in words_porter:
    cur.execute("INSERT INTO words_porter VALUES(%s, %s, %s)", w)

for w in words_mystem:
    cur.execute("INSERT INTO words_mystem VALUES(%s, %s, %s)", w) 

conn.commit()
cur.close()
conn.close()
