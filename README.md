This was auto-migrated from Google Code. Unfortunately, that failed, and now I can't find the original code.
Damn Google, that was really a backstab.

The code here is decompiled from a .jar I found on my web.



---------------------

A tool which transforms a CSV file into another CSV file using a SQL statement.

Many tools spit out CSV, or are able to as one of output options. Also, in hudson, you can very simply log any values you get into bash like echo " $val2, $val2" >> data.csv, for each build or part of a build. So it can be kind of integration tool.

Then you can do quite complex queries – from a flat table, you can actually do subselects and then left joins, which gives you very powerful tool to process the data into something what is ready for plotting as-is – that means, data filtered, cleaned, aggregated, converted, aligned, sorted, etc.

Example:

crunch input.csv output.csv "SELECT AVG(duration) AS durAvg FROM (SELECT * FROM indata ORDER BY duration LIMIT 2 OFFSET 6)"`


---------------------

Hi,

I've crafted a tool which transforms a CSV file into another CSV file using a SQL statement (as I did not found such).

Can be useful for various quick'n'dirty integration during test automation, esp. for data-oriented tasks like perf-tests.

DOWNLOAD

Many tools spit out CSV, or are able to as one of output options. Also, in hudson, you can very simply log any values you get into bash like echo " $val2, $val2" >> data.csv, for each build or part of a build. So it can be kind of integration tool.

Then you can do quite complex queries - from a flat table, you can actually do subselects and then left joins, which gives you very powerful tool to process the data into something what is ready for plotting as-is - that means, data filtered, cleaned, aggregated, converted, aligned, sorted, etc.

That might be my POV, since I like SQL and it's my favorite language not only for querying but also data-oriented procedural programming. But nonetheless, I already shortened my perf test task by ~ 40 minutes of my work for each release. Instead of manual shannanigans in OpenOffice, I run a single command, and voila ;-)

HSQL's syntax: http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html (I was very surprised by HSQL's features, it supports much more of SQL than e.g. MySQL.)

Enjoy :)
