package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.app.ExportArgument
import java.nio.file.Path

/**
 * Info about one file to be created by one SQL query.
 */
data class CruncherOutputPart(
    val outputFile: Path,
    val forExport: ExportArgument,
    val inputTableName: String? = null,
) {

    // These are filled during processing.
    var sql: String? = null
    var columnNamesAndTypes: Map<String, String>? = null
    fun deriveOutputTableName(): String {
        if (inputTableName == null) return Cruncher.TABLE_NAME__OUTPUT
        else return inputTableName + OUTPUT_TABLE_SUFFIX
    }

    companion object {
        const val OUTPUT_TABLE_SUFFIX = "_out"
    }
}