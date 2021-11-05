package cz.dynawest.csvcruncher.app

import org.junit.jupiter.api.Test

class OptionsParsingTest {

    @Test fun testOptionsParsing() {

        val command = "--json | --combineInputs" +
            " |  --exclude=.*/LOAD.*\\.csv" +
            " |  -in  | src/test/data/sample-collab/apollo_recording_group/" +
            " |  -out | target/testResults/apollo_recording_group.csv" +
            " |  -sql | SELECT * FROM apollo_recording_group"

        val arguments = command.split("|").map { it.trim() }
        OptionsParser.parseArgs(arguments.toTypedArray())

    }

}