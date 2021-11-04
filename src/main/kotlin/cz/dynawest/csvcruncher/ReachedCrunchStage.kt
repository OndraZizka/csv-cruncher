package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.util.logger

internal enum class ReachedCrunchStage {
    NONE, INPUT_FILES_PREPROCESSED, INPUT_TABLES_CREATED, OUTPUT_TABLE_CREATED, OUTPUT_TABLE_FILLED, OUTPUT_JSON_CONVERTED;

    fun passed(stage: ReachedCrunchStage): Boolean {
        log.info("$ordinal >= ${stage.ordinal}?")
        return ordinal >= stage.ordinal
    }

    companion object { private val log = logger() }
}