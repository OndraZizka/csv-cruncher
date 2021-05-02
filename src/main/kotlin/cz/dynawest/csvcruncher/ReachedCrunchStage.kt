package cz.dynawest.csvcruncher

import cz.dynawest.csvcruncher.util.logger
import lombok.extern.slf4j.Slf4j
import org.slf4j.Logger

@Slf4j
internal enum class ReachedCrunchStage {
    NONE, INPUT_FILES_PREPROCESSED, INPUT_TABLES_CREATED, OUTPUT_TABLE_CREATED, OUTPUT_TABLE_FILLED, OUTPUT_JSON_CONVERTED;

    fun passed(stage: ReachedCrunchStage): Boolean {
        log.info(String.format("%s >= %s?", ordinal, stage.ordinal))
        return ordinal >= stage.ordinal
    }

    companion object { private val log = logger() }
}