#!/usr/bin/env python
# coding=utf-8

"""
This is a reporting tool.

It prints out reports (in plain text) based on the data in the database.
It is a Python program using the psycopg2 module to connect to the database.
"""

import psycopg2
import time

DBNAME = "news"

db = psycopg2.connect(database=DBNAME)
c = db.cursor()
c.execute("""SELECT title, COUNT(*) AS number
FROM log, articles
WHERE log.status = '200 OK' AND articles.slug = replace(path, '/article/', '')
GROUP BY title
ORDER BY number DESC
LIMIT 3;""")
popular_article = c.fetchall()

c.execute("""SELECT name, COUNT(*) AS number
FROM log, articles, authors
WHERE log.status = '200 OK' AND articles.slug = replace(path, '/article/', '')
AND articles.author = authors.id
GROUP BY name
ORDER BY number DESC
LIMIT 3;""")
popular_author = c.fetchall()

c.execute("""CREATE VIEW v1 AS SELECT time::DATE AS days, COUNT(*) AS number
FROM log
GROUP BY days;
""")

c.execute("""CREATE VIEW v2 AS SELECT time::DATE AS day, COUNT(*) AS num
FROM log
WHERE status != '200 OK'
GROUP BY day;
""")

c.execute("""SELECT day, ROUND(ROUND(num::NUMERIC/number::NUMERIC, 4)*100,2)
FROM v1, v2
WHERE v1.days = v2.day AND ROUND(v2.num::NUMERIC/v1.number::NUMERIC, 4) > 0.01
GROUP BY day, v2.num, v1.number;
""")
errors = c.fetchall()


fo = open("report.txt", "wb")
fo.write("\n===============================================\n")
fo.write("The most popular three articles of all time: \n")
for v in popular_article:
    content = " " + v[0] + " -- " + str(v[1]) + " views\n"
    fo.write(content)

fo.write("\n===============================================\n")
fo.write("The most popular article authors of all time: \n")
for v in popular_author:
    content = " " + v[0] + " -- " + str(v[1]) + " views\n"
    fo.write(content)

fo.write("\n===============================================\n")
fo.write("Days more than 1% of requests lead to errors: \n")
for v in errors:
    content = " " + str(v[0]) + " -- " + str(v[1]) + "% errors\n"
    fo.write(content)

fo.close()
