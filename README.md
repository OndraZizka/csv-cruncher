CSV Cruncher - query and process your CSVs using SQL.
====================================================

A tool which treats CSV files AS SQL tables, and exports CSV files using a SQL `SELECT` statement.

<img src="./docs/images/icon.png" align="left" style="float: left;">

<!--
! [icon](/docs/images/icon.png)
http://static.openheatmap.com/images/googleicon.png
-->

Many tools and systems can export their data to CSV - comma separated values.
Typical work with these data is importing them into some spreadsheet like Excel and process them manually.

<div style="clear: both"></div>


With this tool, you can automate this processing by writing SQL queries, which produce another CSV as a result.
The SQL operations can be quite powerful – just consider what everything you have at hand:

 * Simple selects - ordering, filtering, grouping, aggregating, etc.
 * Joins - use multiple CSV files and use their relations
 * Subselects (pre-computing certain data for another select)
 * Self-joins (same input file used multiple times)
 * Recursive queries
 * Triggers
 * User-defined functions (PL/SQL-based and Java-based)
 * XML and JSON processing
 * Time and date functions
 * Statistical agregate functions

...and more.

All this is backed by [HyperSQL database](http://hsqldb.org/).
See it's very rich [SQL syntax and features documentation](http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html).

Usage
=====

    crunch [-in] <in-CSV>, <in-CSV-2>, ... [-out] <out-CSV> --<option> [-sql] "<SQL>"

Options:

 * -db <pathToDatabaseDirectory>
    Determines where the files of the underlying database will be stored.

 * --rowNumbers[=<firstNumber>|remember]
    Will add a column named `crunchCounter` to the output with unique and incrementing number for each row.
    By specifying `<firstNumber>`, the first number to be used can be set.
    By default, a milliseconds-based timestamp times 1000 is used.
    `remember` is yet to be implemented, and will continue where the last run stopped.

 * --json[=entries|array]
    Create the output in JSON format.
    `entries` (default) will create a JSON entry per line, representing the original rows.
    `array` will create a file with a JSON array (`[...,...]`).

 * --combineInputs[=]

 * --combineDirs[=]

 * --sortInputs[=paramsOrder|alpha|time]


Usage example
=============

    crunch -in myInput.csv -out output.csv \
        -sql "SELECT AVG(duration) AS durAvg FROM (SELECT * FROM myInput ORDER BY duration LIMIT 2 OFFSET 6)"
        --json
        -- j

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


License
=======

In case you really want to use this in your project, then beware:

1. I am not responsible for any bugs in this tool and potential damage it may cause.
2. If you use it, star the repo. I want to be famous :)
3. If you change the source code, make a pull request with your changes.


----------------------------------------------
This was migrated from Google Code.
The code here is decompiled and reverse-engineered from a  .jar I found on my web.
It seems to work, but testing, feedback, and pull requests welcome.

----------------------------------------------

Easter Egg: The original text I sent to JBoss mailing list when introducing the tool :)


> Hi,
>
> I've crafted a tool which transforms a CSV file into another CSV file using a SQL statement (as I did not found such).
>
> Can be useful for various quick'n'dirty integration during test automation, esp. for data-oriented tasks like perf-tests.
>
> [DOWNLOAD](http://ondra.zizka.cz/stranky/programovani/java/apps/CsvCruncher-1.0.jar)
>
> Many tools spit out CSV, or are able to as one of output options. Also, in hudson, you can very simply log any values you get into bash like echo " $val2, $val2" >> data.csv, for each build or part of a build. So it can be kind of integration tool.
>
> Then you can do quite complex queries - from a flat table, you can actually do subselects and then left joins, which gives you very powerful tool to process the data into something what is ready for plotting as-is - that means, data filtered, cleaned, aggregated, converted, aligned, sorted, etc.
>
> That might be my POV, since I like SQL and it's my favorite language not only for querying but also data-oriented procedural programming. But nonetheless, I already shortened my perf test task by ~ 40 minutes of my work for each release. Instead of manual shannanigans in OpenOffice, I run a single command, and voila ;-)
>
> HSQL's syntax: http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html (I was very surprised by HSQL's features, it supports much more of SQL than e.g. MySQL.)
>
> Enjoy :)
