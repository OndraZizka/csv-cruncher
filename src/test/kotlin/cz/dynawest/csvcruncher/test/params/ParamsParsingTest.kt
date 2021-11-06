package cz.dynawest.csvcruncher.test.params

import cz.dynawest.csvcruncher.Options2
import cz.dynawest.csvcruncher.app.OptionsParser
import org.assertj.core.api.Assertions
import org.junit.jupiter.api.Test
import java.nio.file.Path

class ParamsParsingTest {

    @Test fun paramsParsing_singleImport() {

        val opt = parseTestOptions("""-in |  input/path | -itemsAt | /data/children | -out | /output/path | -sql | SELECT * FROM redditAll """)

        Assertions.assertThat(opt.importArguments).size().isEqualTo(1)
        Assertions.assertThat(opt.importArguments.first().path).isEqualTo(Path.of("input/path"))
        Assertions.assertThat(opt.importArguments.first().alias).isNull()
        Assertions.assertThat(opt.importArguments.first().itemsPathInTree).isEqualTo("/data/children")

        Assertions.assertThat(opt.exportArguments).size().isEqualTo(1)
        Assertions.assertThat(opt.exportArguments.first().path).isEqualTo(Path.of("/output/path"))
        Assertions.assertThat(opt.exportArguments.first().alias).isNull()
    }

    @Test fun paramsParsing_twoImports() {

        val opt = parseTestOptions("""-in |  input/path | -itemsAt | /data/children | -in | input2/path | -out | /output/path | -sql | SELECT * FROM redditAll """)

        Assertions.assertThat(opt.importArguments).size().isEqualTo(2)
        Assertions.assertThat(opt.importArguments.first().path).isEqualTo(Path.of("input/path"))
        Assertions.assertThat(opt.importArguments.first().alias).isNull()
        Assertions.assertThat(opt.importArguments.first().itemsPathInTree).isEqualTo("/data/children")
        Assertions.assertThat(opt.importArguments.get(1).path).isEqualTo(Path.of("input2/path"))
        Assertions.assertThat(opt.importArguments.get(1).alias).isNull()
        Assertions.assertThat(opt.importArguments.get(1).itemsPathInTree).isEqualTo("/")

        Assertions.assertThat(opt.exportArguments).size().isEqualTo(1)
        Assertions.assertThat(opt.exportArguments.first().path).isEqualTo(Path.of("/output/path"))
        Assertions.assertThat(opt.exportArguments.first().alias).isNull()
        Assertions.assertThat(opt.exportArguments.first().sqlQuery).isEqualTo("SELECT * FROM redditAll")
    }

    @Test fun paramsParsing_twoExports() {

        val opt = parseTestOptions("""-in |  input/path | -out | /output1/path | -sql | SELECT * FROM redditAll1 | -out | /output2/path | -sql | SELECT * FROM redditAll2 """)

        Assertions.assertThat(opt.importArguments).size().isEqualTo(1)

        Assertions.assertThat(opt.exportArguments).size().isEqualTo(2)
        Assertions.assertThat(opt.exportArguments.get(0).path).isEqualTo(Path.of("/output1/path"))
        Assertions.assertThat(opt.exportArguments.get(0).alias).isNull()
        Assertions.assertThat(opt.exportArguments.get(0).sqlQuery).isEqualTo("SELECT * FROM redditAll1")
        Assertions.assertThat(opt.exportArguments.get(1).path).isEqualTo(Path.of("/output2/path"))
        Assertions.assertThat(opt.exportArguments.get(1).alias).isNull()
        Assertions.assertThat(opt.exportArguments.get(1).sqlQuery).isEqualTo("SELECT * FROM redditAll2")
    }


    private fun parseTestOptions(command: String): Options2 {
        val arguments = command
            .splitToSequence("|")
            .map { obj: String -> obj.trim { it <= ' ' } }
            .filter { x: String -> !x.isEmpty() }
            .toList().toTypedArray()

        val options = OptionsParser.parseArgs(arguments)!!
        return options
    }

}