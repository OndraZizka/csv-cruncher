package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.HsqlDbHelper
import org.apache.commons.lang3.StringUtils
import java.sql.SQLSyntaxErrorException

object HsqldbErrorHandling {

    /**
     * Analyzes the exception against the given DB connection and rethrows an exception with a message containing the available objects as a hint.
     */
    fun throwHintForObjectNotFound(ex: SQLSyntaxErrorException, helper: HsqlDbHelper, sql: String): CsvCruncherException {
        val notFoundIsColumn = analyzeWhatWasNotFound(ex.message!!)
        val tableNames = helper.formatListOfAvailableTables(notFoundIsColumn)
        val hintMsg = if (notFoundIsColumn) """
            |  Looks like you are referring to a column that is not present in the table(s).
            |  Check the header (first line) in the CSV.
            |  Here are the tables and columns are actually available:
            |
            """.trimMargin()
        else """
            |  Looks like you are referring to a table that was not created.
            |  This could mean that you have a typo in the input file name,
            |  or maybe you use --combineInputs but try to use the original inputs.
            |  Or it is this known bug: https://github.com/OndraZizka/csv-cruncher/issues/149
            |  SQL: $sql
            |  These tables are actually available:
            |
            """.trimMargin()

        return CsvCruncherException(
            """$hintMsg$tableNames
                |Message from the database:
                |    ${ex.message}""".trimMargin(), ex)
    }


    /**
     * Tells apart whether the "object not found" was a column or a table.
     * Relies on HSQLDB's exception message, which looks like this:
     * USER LACKS PRIVILEGE OR OBJECT NOT FOUND: JOBNAME IN STATEMENT [SELECT JOBNAME, FROM
     *
     * user lacks privilege or object not found: JOBNAME in statement [SELECT jobName, ... FROM ...]
     *
     * @return true if column, false if table (or something else).
     */
    @JvmStatic
    fun analyzeWhatWasNotFound(message: String): Boolean {
        @Suppress("NAME_SHADOWING")
        var message = message
        var notFoundName = StringUtils.substringAfter(message, "object not found: ")
        notFoundName = StringUtils.substringBefore(notFoundName, " in statement [")
        message = message.uppercase().replace('\n', ' ')

        //String sqlRegex = "[^']*\\[SELECT .*" + notFoundName + ".*FROM.*";
        val sqlRegex = ".*SELECT.*$notFoundName.*FROM.*"
        //LOG.finer(String.format("\n\tNot found object: %s\n\tMsg: %s\n\tRegex: %s", notFoundName, message.toUpperCase(), sqlRegex));
        return message.uppercase().matches(sqlRegex.toRegex())
    }

}