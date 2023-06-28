package cz.dynawest.csvcruncher

import org.junit.jupiter.api.TestInfo
import java.nio.file.Path

fun TestInfo.defaultCsvOutputPath() = "target/results/${testClass.get().simpleName}_${testMethod.get().name}.csv".let { Path.of(it) }

fun TestInfo.defaultJsonOutputPath() = "target/results/${testClass.get().simpleName}_${testMethod.get().name}.json".let { Path.of(it) }
