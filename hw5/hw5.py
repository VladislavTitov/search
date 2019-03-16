import psycopg2
import math
from nltk.corpus import stopwords

# стеммер Портера предназначен только для английских слов, Snowball стеммер является развитием стеммера Портера
from nltk.stem.snowball import SnowballStemmer 

import re
import functools

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")

articles_count = 0
with conn.cursor() as cur:
    cur.execute('SELECT count(*) FROM articles;')
    articles_count = cur.fetchone()[0]

word_included_articles = {}

def prepare(query: str):
    words = re.split(r'\W+', query)
    words = [w.lower() for w in words if w.isalpha()]

    stop_words = set(stopwords.words('russian'))
    words = [w for w in words if not w in stop_words]

    stemmer_eng = SnowballStemmer('english')
    stemmer_rus = SnowballStemmer('russian')
    stemmed = [stemmer_rus.stem(stemmer_eng.stem(word)) for word in words]
    return stemmed

def get_articles(words):
    words = tuple(words)
    articles = {}
    with conn.cursor() as cur: 
        cur.execute("SELECT a.article_id, t.term_text, a.tf_idf FROM article_term a INNER JOIN terms_list t ON a.term_id = t.term_id WHERE t.term_text IN %s", (words,))
        for article_id, term_text, tf_idf in cur:
            if article_id not in articles:
                articles[article_id] = {}
            terms = articles[article_id]
            terms[term_text] = tf_idf
    return articles

def get_query_vector(words):
    vector = {}
    for word in words:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM (SELECT DISTINCT article_id FROM words_porter WHERE term = %s) a", (word,))
            word_included_article = cur.fetchone()[0]
        idf = math.log2(articles_count / word_included_article)
        vector[word] = idf
    return vector

def get_urls_by_article_ids(ids_and_cos):
    url_cos = {}
    for _id, cos in ids_and_cos:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM articles WHERE id = %s", (_id,))
            url = cur.fetchone()[0]
            url_cos[url] = cos
    return url_cos

def search(query: str):
    words = prepare(query)

    articles = get_articles(words)
    query_vector = get_query_vector(words)

    article_cos = {}
    for article_id, terms in articles.items():
        cos = 0
        for term_id, idf in terms.items():
            if term_id in query_vector:
                tf_idf = query_vector[term_id]
                cos += idf*tf_idf
        article_cos[article_id] = cos
    
    article_ids_and_cos = [(k, article_cos[k]) for k in sorted(article_cos, key=article_cos.get, reverse=True)]

    url_cos = get_urls_by_article_ids(article_ids_and_cos)
    
    i = 0
    for url, cos in url_cos.items():
        print(url, cos)
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

