CSV Cruncher - query and process your CSVs using SQL.
====================================================

A tool which treats CSV files as SQL tables, and exports CSV and JSON files using a SQL `SELECT` statement.

<img src="./docs/images/icon.png" align="left" style="float: left;">

<!--
! [icon](/docs/images/icon.png)
http://static.openheatmap.com/images/googleicon.png
-->

Many tools and systems can export their data to CSV - comma separated values.
Typical work with these data is importing them into some spreadsheet like Excel and process them manually.

<div style="clear: both; height: 10px"></div>


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
 ...[and more](http://hsqldb.org/doc/2.0/guide/guide.html).

On top of that, CSV Cruncher can:
 * Convert CSV to JSON
 * Aggregate input files in a directory structure (concatenate, intersect, merge, deduplicate and other operations)
 * Deal with CSV structure changes between files
 * Filter the CSV lines using regular expression
 * Add a unique incrementing number to each row of the result

Features in progress:
 * Import JSON files

And this is planned:
 * Import and export Excel (XLS) files
 * Import any text files, parsed into columns by a regular expression groups
 * Export HTML tables

All this is backed by [HyperSQL database](http://hsqldb.org/).
See it's very rich [SQL syntax and features documentation](http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html).

Download
=====

Download at the [releases page](https://github.com/OndraZizka/csv-cruncher/releases).

Usage
=====

    crunch [-in] <in-CSV>, <in-CSV-2>, ... [-out] <out-CSV> --<option> [-sql] "<SQL>"

### Options:

 * `-in`
    Input paths. Can be files or directories.

 * `-out`
    Output path. Currently only one output table/file is supported.

 * `-sql`
    The SQL operation to be performed. The input files (or the results of preprocessing them) are available as tables.
    See [HSQLDB documentation](http://hsqldb.org/doc/2.0/guide/guide.html#sqlgeneral-chapt) for the vast SQL operations at hand.

 * `-db <pathToDatabaseDirectory>`
    Determines where the files of the underlying database will be stored. Default is `hsqldb/cruncher`.

 * `--include=<regex>`, `--exclude=<regex>`
    Filters which input files are taken as input.
    The whole path relative to the is matched, so make sure to use `.*` at the beginning.
    The `--exclude` is applied after `--include`, so include does not override excluded files.
    If not specified, CSV Cruncher behaves as if `--include` was `.*\.csv$` and `--exclude` had no match.

##### Pre-processing

 * --ignoreLinesMatching=<regEx>
    Ignore lines matching given regular expression.

 * --ignoreFirstLines[=<number>]
    Ignore first `number` lines; the first is considered a header with column names.
    This counts regardless of `ignoreLineRegex`.

 * `--combineInputs\[=concat|intersect|substract]`
    Combine the input files into one file, optionally computing an intersection or substracting one from another.

 * `--combineDirs\[=perDir|perInputDir|perInputSubdir|all]`
    Controls which files are combined together. Default is `all`.
    If the files within one resulting group have different structure (different columns),
    they are automatically divided into subgroups per structure.
    This can be used to process database incremental change logs which sometomes change the schema.

 * `--sortInputs\[=paramsOrder|alpha|time]`
    Controls how files are sorted before combining, and in which order the tables are created.

    Read the logs or use `-sql SELECT ... FROM INFORMATION_SCHEMA.*` to study the schema created after preprocessing.

##### Post-processing

 * `--rowNumbers\[=<firstNumber>|remember]`
    Will add a column named `crunchCounter` to the output with unique and incrementing number for each row.
    By specifying `<firstNumber>`, the first number to be used can be set.
    By default, a milliseconds-based timestamp times 1000 is used.
    `remember` is yet to be implemented, and will continue where the last run stopped.

 * `--json\[=entries|array]`
    Create the output in JSON format.
    `entries` (default) will create a JSON entry per line, representing the original rows.
    `array` will create a file with a JSON array (`[...,...]`).


This README may be slightly obsolete; For a full list of options, check the
[`Options`](https://github.com/OndraZizka/csv-cruncher/blob/master/src/main/java/cz/dynawest/csvcruncher/Options.java) class.


Usage example
=============

Simple SQL operation on a single CSV file:

    crunch -in myInput.csv -out output.csv
        -sql "SELECT AVG(duration) AS durAvg FROM (SELECT * FROM myInput ORDER BY duration LIMIT 2 OFFSET 6)"
        --json

With input files searched in a directory and concatenated:

    crunch
        -in src/test/data/sampleMultiFilesPerDir/apollo_session/
        -out target/results/result.csv
        --json=entries
        --rowNumbers
        --combineInputs=concat
        --combineDirs=all
        -sql 'SELECT session_uid, name, session_type, created_time, modified_date
              FROM concat_1 ORDER BY session_type, created_time DESC'

With input files searched in subdirectories of a directory, concatenated, and used as table-per-subdirectory:

    (Supported, but example to be added)


Data and usage example
======================

CSV file named `eapData.csv`:

    ## jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale
    'eap-5.1.0-perf-deployers', 355,'production','testdata/war/hellothere.war','hellothere.war',10282,14804,1000
    'eap-5.1.0-perf-deployers', 355,'production','testdata/ear/EarWithWar-Counter.ear','EarWithWar-Counter.ear',11005,18904,1000
    'eap-5.1.0-perf-deployers', 355,'production','testdata-own/war/war-big-1.0.war','war-big-1.0.war',1966,14800,100
    ...

That would create a table named `eapData` (unless concatenation is used).
So you may issue such SQL query:

    SELECT jobName, buildNumber, config, ar, arFile, deployDur, warmupDur, scale,
            CAST(warmupDur AS DOUBLE) / CAST(deployDur AS DOUBLE) AS warmupSlower
     FROM eapData ORDER BY deployDur


License
=======

In case you use this in your project, then beware:

1. I am not responsible for any bugs in this tool and potential damage it may cause.
2. If you use it, star the repo. I want to be famous :)
3. If you change the source code, make a pull request with your changes.
4. Consider donating to [HSQLDB "SupportWare"](http://hsqldb.org/web/supportware.html).


*Easter Egg: The original text I sent to JBoss mailing list when introducing the tool in 2011 :)*

> Hi,
>
> I've crafted a tool which transforms a CSV file into another CSV file using a SQL statement (as I did not find such).
>
> Can be useful for various quick'n'dirty integration during test automation, esp. for data-oriented tasks like perf-tests.
>
> [DOWNLOAD](http://ondra.zizka.cz/stranky/programovani/java/apps/CsvCruncher-1.0.jar)
>
> Many tools spit out CSV, or are able to as one of output options. Also, in Hudson, you can very simply log
  any values you get into bash like echo " $val2, $val2" >> data.csv, for each build or part of a build.
  So it can be kind of integration tool.
>
> Then you can do quite complex queries - from a flat table, you can sactually do subselects and then left joins,
  which gives you very powerful tool to process the data into something what is ready for plotting as-is - that means,
  data filtered, cleaned, aggregated, converted, aligned, sorted, etc.
>
> That might be my POV, since I like SQL and it's my favorite language not only for querying
  but also data-oriented procedural programming. But nonetheless, I already shortened my perf test task
  by ~ 40 minutes of my work for each release. Instead of manual shannanigans in OpenOffice, I run a single command, and voila ;-)
>
> HSQL's syntax: http://hsqldb.org/doc/2.0/guide/dataaccess-chapt.html (I was very surprised by HSQL's features,
  it supports much more of SQL than e.g. MySQL.)
>
> Enjoy :)

<img src="http://c1.navrcholu.cz/hit?site=144925;t=lb14;ref=;jss=0" width="14" height="14" alt="NAVRCHOLU.cz" style="border:none" />
