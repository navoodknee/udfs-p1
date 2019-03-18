#!/usr/bin/python2
#-*- coding: UTF-8 -*-
"""Udacity Full Stack Web Developer Nano Degree
Project 1: SQL Queries
Developer: David Nadwdodny
Date 03/15/2019
"""

import psycopg2
""" Question 1

1. What are the most popular three articles of all time? Which articles have
 been accessed the most? Present this information as a sorted list with the
 most popular article at the top.

Example:

"Princess Shellfish Marries Prince Handsome" — 1201 views
"Baltimore Ravens Defeat Rhode Island Shoggoths" — 915 views
"Political Scandal Ends In Political Scandal" — 553 views"""

"""  This query uses regex to snip /articles/ from the slug table in log which
allows us to link log to article via the slug field. We filter for articles
that were successfully accessed and no requests that weren't articles. Finally
we group by title and count to generate the required view count.
"""
questionOneQuery = """SELECT title, count(*) FROM
    (select log.id, REGEXP_REPLACE(path, '/article/', '') as shortpath from log
     WHERE status = '200 OK'  and path !='/') as subq
JOIN articles ON (shortpath = slug)
GROUP BY title
ORDER BY count DESC;"""

db = psycopg2.connect(database="news")
c = db.cursor()
c.execute(questionOneQuery)
print("\n---[ Results for Question 1 ]---\n")
for record in c:
    print(record[0] + " - " + str(record[1]) + " views")
db.close()

""" Question 2

2. Who are the most popular article authors of all time? That is, when you sum
 up all of the articles each author has written, which authors get the most
 page views? Present this as a sorted list with the most
 popular author at the top.

The challenge:

How to link
 author -> article -> log

author -> article

select name, author, title, slug from articles join authors on (author=authors.id);

arttcle -> log

SELECT * FROM
    (select log.id, REGEXP_REPLACE(path, '/article/', '') as shortpath from log
     WHERE status = '200 OK'  and path !='/') as subq
JOIN articles ON (shortpath = slug)) as subq2; LIMIT 20

# allll together now..

SELECT  name, count(*) FROM
    (SELECT * FROM
        (SELECT log.id, REGEXP_REPLACE(path, '/article/', '') as shortpath FROM log
         WHERE status = '200 OK'  AND path !='/') as subq
    JOIN articles ON (shortpath = slug)) as subq2
JOIN authors ON (author=authors.id)
GROUP BY name
ORDER BY count DESC


Example:

Ursula La Multa — 2304 views
Rudolf von Treppenwitz — 1985 views
Markoff Chaney — 1723 views
Anonymous Contributor — 1023 views
"""

questionTwoQuery = """SELECT  name, count(*) FROM
    (SELECT * FROM
        (SELECT log.id, REGEXP_REPLACE(path, '/article/', '') as shortpath FROM log
         WHERE status = '200 OK' AND path !='/') as subq
    JOIN articles ON (shortpath = slug)) as subq2
JOIN authors ON (author=authors.id)
GROUP BY name
ORDER BY count DESC;"""

db = psycopg2.connect(database="news")
c = db.cursor()
c.execute(questionTwoQuery)
print("\n---[ Results for Question 2 ]---\n")
for record in c:
    print(record[0] + " - " + str(record[1]) + " views")
db.close()

""" Question 3

3. On which days did more than 1% of requests lead to errors? The log table
includes a column status that indicates the HTTP status code that the
news site sent to the user's browser. (Refer to this lesson for
more information about the idea of HTTP status codes.)

REAL QUESTION: How to get % of non 200s ?

Sub query formats date to truncate down to just Month day Year, which
allows the outer query to group by days and status codes. Python is utilized
for the heavier logic lifting and we have the answer to question 3.

Example:

July 29, 2016 — 2.5% errors"""
errorThreshold = 1.0

questionTwoQuery = """SELECT time, status, count(*) FROM
    (SELECT status, to_char(time, 'MM DD YYYY') AS time FROM log) AS subq
GROUP BY time, status;"""

db = psycopg2.connect(database="news")
c = db.cursor()
c.execute(questionTwoQuery)
print("\n---[ Results for Question 3 ]---\n")

i = 0
while(i < c.rowcount):
    day = c.fetchmany(2)
    date = day[0][0]
    totalRequests = day[0][2]
    totalErrors = day[1][2]
    percentError = (100 * float(totalErrors)) / float(totalRequests)
    if(percentError > errorThreshold):
        print("Date: %s - %.2f%% errors\n" % (date, percentError))
    i = i + 2

db.close()
