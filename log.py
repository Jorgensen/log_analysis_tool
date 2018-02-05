import psycopg2
import time

DBNAME = "news"

db = psycopg2.connect(database=DBNAME)
c = db.cursor()
c.execute("""select title, count(*) as number from log, articles
where log.status = '200 OK' and articles.slug = replace(path, '/article/', '')
group by title
order by number desc
limit 3;""")
popular_article = c.fetchall() 

c.execute("""select name, count(*) as number from log, articles, authors
where log.status = '200 OK' and articles.slug = replace(path, '/article/', '') and articles.author = authors.id
group by name
order by number desc
limit 3;""")
popular_author = c.fetchall() 

c.execute("""create view v1 as select time::date as days, count(*) as number from log 
group by days
;
""")

c.execute("""create view v2 as select time::date as day, count(*) as num from log
where status != '200 OK'
group by day
;
""")

c.execute("""select day, round(round(num::numeric/number::numeric, 4)*100,2) from v1, v2
where v1.days = v2.day and round(v2.num::numeric/v1.number::numeric, 4) > 0.01
group by day, v2.num, v1.number
;
""")
errors = c.fetchall() 

# print "==============================================="
# print "The most popular three articles of all time: "
# for v in popular_article:
#     print " ",v[0],"--", v[1],"views"
# 
# 
# print "==============================================="
# print "The most popular article authors of all time: "
# for v in popular_author:
#     print " ",v[0],"--", v[1],"views"
# 
# 
# print "==============================================="
# print "Days more than 1% of requests lead to errors: "
# for v in errors:
#     print " ",v[0],"--", v[1],"% errors"

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