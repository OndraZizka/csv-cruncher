package cz.dynawest.csvcruncher

import lombok.Data
import java.nio.file.Path

/**
 * One part of input data, maps to one or more SQL tables. Can be created out of multiple input files.
 */
@Data
data class CruncherInputSubpart(
    private val originalInputPath: Path? = null,
    val combinedFile: Path,
    val combinedFromFiles: List<Path>? = null,
    var tableName: String? = null,
) {

    companion object {
        fun trivial(path: Path): CruncherInputSubpart {
            val cis = CruncherInputSubpart(
                    originalInputPath = path,
                    combinedFile = path,
                    combinedFromFiles = listOf(path),
            )
            return cis
        }
    }
}