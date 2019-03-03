import psycopg2

# предполагается установленный модуль nltk
import nltk

# скачиваем нужные компоненты, при вызове этой команды откроется gui интерфейс, нужно выбрать popular (либо all) 
nltk.download()

from nltk.corpus import stopwords

# стеммер Портера предназначен только для английских слов, Snowball стеммер является развитием стеммера Портера
from nltk.stem.snowball import SnowballStemmer 

import re
import functools

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")

def prepare(query: str):
    words = re.split(r'\W+', query)
    words = [w.lower() for w in words if w.isalpha()]

    stop_words = set(stopwords.words('russian'))
    words = [w for w in words if not w in stop_words]

    stemmer_eng = SnowballStemmer('english')
    stemmer_rus = SnowballStemmer('russian')
    stemmed = [stemmer_rus.stem(stemmer_eng.stem(word)) for word in words]
    return stemmed

def sort_by_includes(words):
    sql = "select count(article_id) from article_term a INNER JOIN (select * from terms_list where term_text = %s) t ON a.term_id = t.term_id;"
    with conn.cursor() as cur:
        word_includes = []
        for word in words:
            cur.execute(sql, (word,))
            includes = cur.fetchone()[0]
            word_includes.append((word, includes))
        return [i[0] for i in sorted(word_includes, key= lambda w: w[1], reverse=True)]

def search(query: str):
    q_words = sort_by_includes(prepare(query))
    sql = "select article_id from article_term a INNER JOIN (select * from terms_list where term_text = %s) t ON a.term_id = t.term_id;"
    includes = []
    for word in q_words:
        s = set()
        with conn.cursor() as cur:
            cur.execute(sql, (word,))
            for item in cur:
                s.add(item[0])
        includes.append(s)
        
    intersection = list(functools.reduce(lambda x,y: x&y, includes))
    titles = []
    for item in intersection:
        with conn.cursor() as cur:
            cur.execute("SELECT title from articles WHERE id = %s", (item,))
            t = cur.fetchone()[0]
            titles.append(t)

    print(titles)

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
