import psycopg2
import math
import functools
from nltk.corpus import stopwords
# стеммер Портера предназначен только для английских слов, Snowball стеммер является развитием стеммера Портера
from nltk.stem.snowball import SnowballStemmer
import re

import numpy as np

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")


def fetchone(statement, params=None):
    with conn.cursor() as cur:
        if params == None:
            cur.execute(statement)
        else:
            cur.execute(statement, params)
        return cur.fetchone()


def fetch(statement, params=None):
    data = []
    with conn.cursor() as cur:
        if params is None:
            cur.execute(statement)
        else:
            cur.execute(statement, params)
        for row in cur:
            data.append(row)
    return data


k = 5
terms = []
articles = []

articles_count = fetchone('SELECT count(*) FROM articles;')[0]

words_count = fetchone("SELECT count(*) FROM terms_list;")[0]


def lsa():
    global terms
    global articles
    matrix = np.zeros((words_count, articles_count))
    article_term = fetch("SELECT * FROM article_term")
    for term_id, article_id, tf_idf in article_term:
        try:
            term_index = terms.index(term_id)
        except ValueError:
            terms.append(term_id)
            term_index = len(terms) - 1
        try:
            article_index = articles.index(article_id)
        except ValueError:
            articles.append(article_id)
            article_index = len(articles) - 1
        matrix[term_index][article_index] = tf_idf
    u, s, v = np.linalg.svd(np.array(matrix), full_matrices=False)
    global k
    return u[:, :k], s[:k], v[:k]


def get_articles(words):
    words = tuple(words)
    articles = []
    with conn.cursor() as cur:
        cur.execute(
            "SELECT a.article_id FROM article_term a INNER JOIN terms_list t ON a.term_id = t.term_id WHERE t.term_text IN %s",
            (words,))
        for row in cur:
            articles.append(row[0])
    return articles


def prepare(query: str):
    words = re.split(r'\W+', query)
    words = [w.lower() for w in words if w.isalpha()]

    stop_words = set(stopwords.words('russian'))
    words = [w for w in words if w not in stop_words]

    stemmer_eng = SnowballStemmer('english')
    stemmer_rus = SnowballStemmer('russian')
    stemmed = [stemmer_rus.stem(stemmer_eng.stem(word)) for word in words]
    return stemmed


words_ids = {}


def get_id_for_term(term):
    global words_ids
    if term not in words_ids:
        words_ids[term] = fetchone("select term_id from terms_list where term_text = %s", (term,))[0]
    return words_ids[term]


def generate_query_vector(words, terms_count):
    global terms
    global articles
    query = np.zeros((terms_count,))
    for word in words:
        try:
            term_index = terms.index(get_id_for_term(word))
            query[term_index] = 1
        except:
            pass
    return query


def get_url_by_index(idx):
    global articles
    article_id = articles[idx]
    url = fetchone("SELECT url FROM articles WHERE id=%s", (article_id,))[0]
    return url


def search(query):
    words = prepare(query)

    u, s, v = lsa()

    query_vec = generate_query_vector(words, len(u))

    if query_vec.sum() <= 0:
        print("Nothing is found")
        return

    query_vec = query_vec.dot(u).dot(np.linalg.inv(np.diag(s)))

    article_rate = {}
    for i in range(v.shape[1]):
        d = v[:, i]
        rate = query_vec.dot(d) / (np.linalg.norm(query_vec) * np.linalg.norm(d))
        article_rate[i] = rate

    article_rate = [(k, article_rate[k]) for k in sorted(article_rate, key=article_rate.get, reverse=True)]

    i = 0
    for idx, rate in article_rate:
        url = get_url_by_index(idx)
        print(url, rate)
        i += 1
        if i == 10:
            break


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 2:
        print("Number of arguments is too many!")
        conn.close()
        sys.exit()

    if len(sys.argv) <= 1:
        print("Enter your query: ")
        query = input()
    else:
        query = sys.argv[1]

    search(query)
    conn.close()
