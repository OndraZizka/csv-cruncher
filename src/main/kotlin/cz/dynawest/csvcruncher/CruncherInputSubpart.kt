package cz.dynawest.csvcruncher

import java.nio.file.Path
import kotlin.io.path.nameWithoutExtension

/**
 * One part of input data, maps to one or more SQL tables. Can be created out of multiple input files.
 */
data class CruncherInputSubpart(
    val originalImportArgument: ImportArgument?,
    private val originalInputPath: Path? = null,
    val combinedFile: Path,
    val combinedFromFiles: List<Path>? = null,
    var tableName: String? = null,
) {

    companion object {
        fun trivial(importArgument: ImportArgument): CruncherInputSubpart {
            val path = importArgument.path
            require(path != null) { "importArgument.path not expected to be null here. File a bug. $importArgument" }
            val cis = CruncherInputSubpart(
                    originalImportArgument = importArgument,
                    originalInputPath = path,
                    combinedFile = path,
                    combinedFromFiles = listOf(path),
                    tableName = path.fileName.nameWithoutExtension,
            )
            return cis
        }
    }
}