package cz.dynawest.csvcruncher.util

import cz.dynawest.csvcruncher.HsqlDbHelper


object SqlFunctions {

    fun defineSqlFunctions(hsqlDbHelper: HsqlDbHelper) {
        hsqlDbHelper.executeSql(
            """CREATE FUNCTION startsWith(whole LONGVARCHAR, startx LONGVARCHAR) RETURNS BOOLEAN LANGUAGE JAVA DETERMINISTIC NO SQL
                    EXTERNAL NAME 'CLASSPATH:cz.dynawest.csvcruncher.util.SqlFunctions.startsWith'""",
            "Error creating Java function."
        )
    }

    @JvmStatic
    fun startsWith(whole: String, startx: String): Boolean {
        return whole.startsWith(startx)
    }
}