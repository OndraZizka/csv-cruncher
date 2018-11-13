package cz.dynawest.csvcruncher;

import java.util.logging.Logger;

enum ReachedCrunchStage
{
    NONE,
    INPUT_FILES_PREPROCESSED,
    INPUT_TABLES_CREATED,
    OUTPUT_TABLE_CREATED,
    OUTPUT_TABLE_FILLED,
    OUTPUT_JSON_CONVERTED;

    private static final Logger LOG = Logger.getLogger(ReachedCrunchStage.class.getName());

    public boolean passed(ReachedCrunchStage stage)
    {
        LOG.info(String.format("%s >= %s?", this.ordinal(), stage.ordinal()));
        return (this.ordinal() >= stage.ordinal());
    }
}
