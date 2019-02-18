import codecs
import uuid
import psycopg2
import requests
from  parsel import Selector

links = []
for i in range(1, 4):
    r = requests.get('https://losst.ru/page/{}'.format(i))
    sel = Selector(text=r.text)

    for l in sel.xpath("//h2[@class]/a/@href"):
        links.append(l.get())

posts = []
for l in links:
    r = requests.get(l)
    print(r.encoding)
    sel = Selector(text=codecs.decode(r.text.encode(encoding=r.encoding), 'utf-8'))
    
    title = sel.xpath('//h1[@class="title single-title entry-title"]/text()').get()
    print(title)

    keywords = ""
    for tag in sel.xpath('//div[@class="tags border-bottom"]/a/text()'):
        keywords += (tag.get() + ";")
    print(keywords)

    post_div = sel.xpath('//div[@class="post-single-content box mark-links entry-content"]')

    post_items = post_div.xpath('.//p/text() | .//h2/text() | .//h3/text() | .//span/text() | .//a/text() | .//img/@src | .//strong/text() | .//code/text() | .//ul/li/text() | .//ol/li/text()')

    content = ""
    for p in post_items:
        text = str(p.get()).strip()
        if len(text) < 1:
            continue
        content += text + '\n' # if text[-1] in ['.', '!', '?', ':', ';'] else text + ' '
    print(content)
    posts.append((title, keywords, content, l))

# db = postgresql.open('pq://vlados:123456@localhost:5433/search_1')

conn = psycopg2.connect(database="search_1", user="vlados", password="123456", host="localhost", port="5433")
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS students (id SERIAL PRIMARY KEY, name VARCHAR(32), surname VARCHAR(32), mygroup VARCHAR(6))")
cur.execute("CREATE TABLE IF NOT EXISTS articles (id uuid PRIMARY KEY, title VARCHAR(256), keywords VARCHAR(256), content TEXT, url VARCHAR(128), student_id INT REFERENCES students (id))")

cur.execute("INSERT INTO students(id, name, surname, mygroup) VALUES (116,'Владислав', 'Титов', '11-502')")

# ins = db.prepare("INSERT INTO articles (title, keywords, content, url, student_id) VALUES ($1, $2, $3, $4, 116)")
for post in posts:
    cur.execute("INSERT INTO articles (id, title, keywords, content, url, student_id) VALUES (%s, %s, %s, %s, %s, 116)", (str(uuid.uuid4()), post[0], post[1], post[2], post[3]))
    # ins(post[0], post[1], post[2], post[3])

conn.commit()
cur.close()
conn.close()
