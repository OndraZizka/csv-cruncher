This was auto-migrated from Google Code. Unfortunately, that failed, and now I can't find the original code.
Damn Google, that was really a backstab.

The code here is decompiled from a .jar I found on my web.

I've only made the code buildable, formatted some classes and renamed few variables.
I didn't try running it yet so it may be unusable at the moment.
Pull requests welcome.

If it was useful to you, star this github repo :)

---------------------

A tool which treats CSV files AS SQL tables, and exports CSV files using a SQL SELECT statement.

Many tools spit out CSV, or are able to as one of output options.
Also, in your projects, you can simply use the logging to put some values into a CSV, for instance in Bash:
`echo "$val2, $val2" >> data.csv`

Then you can do quite complex queries – even with a single flat table, you can actually do subselects and then LEFT JOINs,
which gives you very powerful tool to process the data.

Usage
=====

    crunch [-in] <in-CSV>, <in-CSV-2>, ... [-out] <out-CSV> [-sql] "<SQL>"

(The `crunch` script yet to be done. Currently you need to run `java -jar CsvCruncher.jar <args>`.)

Usage example
=============

    crunch input.csv output.csv \
        "SELECT AVG(duration) AS durAvg FROM (SELECT * FROM indata ORDER BY duration LIMIT 2 OFFSET 6)"

Data and usage example
======================

CSV data

    ## jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale
    'eap-5.1.0-perf-deployers', 355,'production','testdata/war/hellothere.war','hellothere.war',10282,14804,1000
    'eap-5.1.0-perf-deployers', 355,'production','testdata/ear/EarWithWar-Counter.ear','EarWithWar-Counter.ear',11005,18904,1000
    'eap-5.1.0-perf-deployers', 355,'production','testdata-own/war/war-big-1.0.war','war-big-1.0.war',1966,14800,100
    ...

SQL query:

    SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
            CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
        FROM indata ORDER BY deployDur



---------------------

Hi,

I've crafted a tool which transforms a CSV file into another CSV file using a SQL statement (as I did not found such).

Can be useful for various quick'n'dirty integration during test automation, esp. for data-oriented tasks like perf-tests.

[DOWNLOAD](http://ondra.zizka.cz/stranky/programovani/java/apps/CsvCruncher-1.0.jar)

Many tools spit out CSV, or are able to as one of output options. Also, in hudson, you can very simply log any values you get into bash like echo " $val2, $val2" >> data.csv, for each build or part of a build. So it can be kind of integration tool.

Then you can do quite complex queries - from a flat table, you can actually do subselects and then left joins, which gives you very powerful tool to process the data into something what is ready for plotting as-is - that means, data filtered, cleaned, aggregated, converted, aligned, sorted, etc.

That might be my POV, since I like SQL and it's my favorite language not only for querying but also data-oriented procedural programming. But nonetheless, I already shortened my perf test task by ~ 40 minutes of my work for each release. Instead of manual shannanigans in OpenOffice, I run a single command, and voila ;-)

HSQL's syntax: http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html (I was very surprised by HSQL's features, it supports much more of SQL than e.g. MySQL.)

Enjoy :)


