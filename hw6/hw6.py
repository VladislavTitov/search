import psycopg2
import math
import functools
from nltk.corpus import stopwords
# стеммер Портера предназначен только для английских слов, Snowball стеммер является развитием стеммера Портера
from nltk.stem.snowball import SnowballStemmer 
import re

k = 1.2
b = 0.75

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")

articles_count = 0
with conn.cursor() as cur:
    cur.execute('SELECT count(*) FROM articles;')
    articles_count = cur.fetchone()[0]

article_words_count = {}
with conn.cursor() as cur:
    cur.execute('SELECT article_id, count(term) as c FROM words_porter GROUP BY article_id;')
    for row in cur:
        article_words_count[row[0]] = row[1]

avgdl = functools.reduce(lambda x, y: x + y, article_words_count.values()) / articles_count

def tf(term, article_id):
    with conn.cursor() as cur:
        cur.execute("select count(id) from words_porter group by term, article_id HAVING term = %s AND article_id=%s;", (term, article_id))
        try:
            document_word_includes_count = cur.fetchone()[0]
        except:
            document_word_includes_count = 0
    global article_words_count
    tf = document_word_includes_count / article_words_count[article_id]
    return tf

def idf(term):
    with conn.cursor() as cur:
        cur.execute("SELECT count(*) FROM (SELECT DISTINCT article_id FROM words_porter WHERE term = %s) a", (term,))
        word_included_article = cur.fetchone()[0]
    
    global articles_count
    idf = math.log2((articles_count - word_included_article + 0.5) / (word_included_article + 0.5))
    return idf

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
    articles = []
    with conn.cursor() as cur: 
        cur.execute("SELECT a.article_id FROM article_term a INNER JOIN terms_list t ON a.term_id = t.term_id WHERE t.term_text IN %s", (words,))
        for row in cur:
            articles.append(row[0])
    return articles

def get_urls_by_article_ids(ids_and_cos):
    url_cos = {}
    for _id, cos in ids_and_cos:
        with conn.cursor() as cur:
            cur.execute("SELECT url FROM articles WHERE id = %s", (_id,))
            url = cur.fetchone()[0]
            url_cos[url] = cos
    return url_cos

def search(query):
    words = prepare(query)
    articles = get_articles(words)
    global articles_count
    global avgdl
    global k
    global b

    article_score = []
    for article in articles:
        score = 0
        for word in words:
            prev_score = idf(word) * (tf(word, article) * (k + 1)) / (tf(word, article) + k * (1 - b + b*articles_count / avgdl))
            if prev_score > 0:
                score += prev_score
        article_score.append((article, score))

    article_score = sorted(article_score, key=lambda a: a[1], reverse=True)

    url_cos = get_urls_by_article_ids(article_score)
    
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

