import psycopg2
import uuid

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS terms_list(term_id uuid, term_text TEXT UNIQUE)")
cur.execute("CREATE TABLE IF NOT EXISTS article_term(term_id uuid, article_id uuid)")

cur.execute("SELECT * FROM words_porter")

term_arcticles = {}

for item in cur:
    word = item[1]
    article_id = item[2]
    if not word in term_arcticles:
        term_arcticles[word] = set()
    term_arcticles[word].add(article_id)

for term, articles in term_arcticles.items():
    term_id = str(uuid.uuid4())
    cur.execute("INSERT INTO terms_list VALUES(%s, %s)", (term_id, term))

    for a in articles:
        cur.execute("INSERT INTO article_term(term_id, article_id) VALUES(%s, %s)", (term_id, a))

conn.commit()
cur.close()
conn.close()
