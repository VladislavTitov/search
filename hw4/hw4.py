import psycopg2
import math

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")

with conn.cursor() as cur: 
    cur.execute('ALTER TABLE IF EXISTS article_term ADD COLUMN tf_idf double precision NOT NULL DEFAULT 0.0;')
    conn.commit()

articles_count = 0
with conn.cursor() as cur:
    cur.execute('SELECT count(*) FROM articles;')
    articles_count = cur.fetchone()[0]

article_words_count = {}
with conn.cursor() as cur:
    cur.execute('SELECT article_id, count(term) as c FROM words_porter GROUP BY article_id;')
    for row in cur:
        article_words_count[row[0]] = row[1]

terms_articles = []
with conn.cursor() as cur:
    cur.execute('select t.term_id, term_text, article_id from article_term a INNER JOIN terms_list t ON a.term_id = t.term_id;')
    for row in cur:
        terms_articles.append((row[0], row[1], row[2]))

word_included_articles = {}
tf_idf_matrix = []

for term_id, term, article_id in terms_articles:
    with conn.cursor() as cur:
        cur.execute("select count(id) from words_porter group by term, article_id HAVING term = %s AND article_id=%s;", (term, article_id))
        document_word_includes_count = cur.fetchone()[0]

    tf = document_word_includes_count / article_words_count[article_id]

    if term not in word_included_articles:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM (SELECT DISTINCT article_id FROM words_porter WHERE term = %s) a", (term,))
            word_included_articles[term] = cur.fetchone()[0]
    word_included_article = word_included_articles[term]
    idf = math.log2(articles_count / word_included_article)

    tf_idf_matrix.append((tf*idf, term_id, article_id))

for row in tf_idf_matrix:
    with conn.cursor() as cur:
        cur.execute('UPDATE article_term SET tf_idf = %s WHERE term_id = %s AND article_id = %s', row)

conn.commit()
conn.close()

