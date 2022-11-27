package cz.dynawest.csvcruncher.converters.spreadsheet

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.converters.*
import cz.dynawest.csvcruncher.converters.spreadsheet.SheetUtils.checkRangesDoOverlap
import cz.dynawest.csvcruncher.converters.spreadsheet.SheetUtils.columnIndexToLettersName
import cz.dynawest.csvcruncher.converters.spreadsheet.SheetUtils.computeBoundingRange
import cz.dynawest.csvcruncher.converters.spreadsheet.SheetUtils.getSheetByNameOrIndex
import org.apache.poi.ss.usermodel.*
import org.apache.poi.ss.usermodel.CellType.*
import java.io.InputStream
import java.nio.file.Path
import kotlin.io.path.inputStream
import kotlin.io.path.outputStream
import kotlin.math.max


class SpreadsheetFileTabularizer : FileTabularizer {

    override fun convert(inputPath: Path, formatSpecificItemsLocation: String): Path {

        val sheetNameOrIndex = formatSpecificItemsLocation.substringBefore("!", missingDelimiterValue = "")
        val givenRange = RectRange.parse(formatSpecificItemsLocation.substringAfter("!"))

        // 1st pass - Identify the columns.
        val propertiesMetadataCollector = TabularPropertiesMetadataCollector()

        val exportingContext: ConvertingContext

        inputPath.inputStream().use { iS ->
            val wb: Workbook = WorkbookFactory.create(iS)
            val sheet: Sheet = wb.getSheetByNameOrIndex(sheetNameOrIndex)

            val actualBoundingRange = computeBoundingRange(sheet)
            givenRange?.let { checkRangesDoOverlap(it, actualBoundingRange) }

            val rangeReference = RangeReference(inputPath, sheetNameOrIndex, givenRange)

            // TBD Find the real first row.
            val firstRowIndex = max(sheet.firstRowNum, givenRange?.rows?.first ?: 0)

            val colNames = sheet.getRow(firstRowIndex).associate { it.columnIndex to cellAsString(it) }

            propertiesMetadataCollector.beforeEntries()
            for (row in sheet) {
                val propertySequence = row.asSequence().map { cell ->
                    // Find the column name, or use A, B, C...
                    val colName = colNames[cell.columnIndex] ?: columnIndexToLettersName(cell.columnIndex)
                    return@map when (cell.cellType) {
                        _NONE -> CrunchProperty.Null(colName)
                        NUMERIC -> CrunchProperty.Number(colName, cell.numericCellValue)
                        STRING -> CrunchProperty.String(colName, cell.stringCellValue)
                        FORMULA -> CrunchProperty.Expression(colName, cell.cellFormula)
                        BLANK -> CrunchProperty.String(colName, "")
                        BOOLEAN -> CrunchProperty.Boolean(colName, cell.booleanCellValue)
                        ERROR -> CrunchProperty.Expression(colName, "Error-${cell.errorCellValue}")
                        else -> CrunchProperty.Null(colName)
                    }
                }
                propertiesMetadataCollector.processEntry(FlattenedEntrySequence(propertySequence))
            }
            propertiesMetadataCollector.afterEntries()

            exportingContext = ConvertingContext(rangeReference, actualBoundingRange, colNames)
            null
        }

        // 2nd pass - Export. The exporter should be a member or a parameter.
        val outputPath = deriveOutputPath(inputPath)
        outputPath.outputStream().use { outputStream ->
            val csvExporter = CsvExporter(outputStream, propertiesMetadataCollector.propertiesSoFar)

            csvExporter.beforeEntries()
            visitEntries(exportingContext, csvExporter)
            csvExporter.afterEntries()

            outputStream.flush()
        }
        return outputPath
    }


    /** Split into two functions for the sake for testability. */
    private fun visitEntries(exportingContext: ConvertingContext, csvExporter: EntryProcessor) {
        exportingContext.givenRange.inputPath ?: throw CsvCruncherException("An input path missing in given range ${exportingContext.givenRange}. Likely a bug.")
        exportingContext.givenRange.inputPath.inputStream().use { iS ->
            visitEntries(exportingContext, csvExporter, iS)
        }
    }

    fun visitEntries(exportingContext: ConvertingContext, csvExporter: EntryProcessor, iS: InputStream) {
        val wb: Workbook = WorkbookFactory.create(iS)
        val sheet: Sheet = wb.getSheetByNameOrIndex(exportingContext.givenRange.sheetNameOrIndex)
        val givenRowsRange = exportingContext.givenRange.range?.rows ?: IntRange_MAX
        for (row in sheet) {
            if (row.rowNum !in givenRowsRange) continue
            val entry = convertRow(row, exportingContext)
            csvExporter.processEntry(entry)
        }
    }


    data class ConvertingContext(
        val givenRange: RangeReference,
        val actualBoundingRange: RectRange,
        val colNames: Map<Int, String> = mapOf(),
        val currentRowIndex: Int = 0,
    )

    private fun convertRow(row: Row, convertingContext: ConvertingContext): FlattenedEntrySequence {
        val fieldsFlattened: Sequence<CrunchProperty> = row.asSequence().flatMap { cell ->
            val colName = convertingContext.colNames[cell.columnIndex] ?: columnIndexToLettersName(cell.columnIndex)
            when (cell.cellType) {
                _NONE -> sequenceOf( CrunchProperty.Null(colName) )
                NUMERIC -> sequenceOf( CrunchProperty.Number(colName, cell.numericCellValue) )
                STRING -> sequenceOf( CrunchProperty.String(colName, cell.stringCellValue) )
                FORMULA -> sequenceOf( CrunchProperty.Expression(colName, cell.cellFormula) )
                BLANK -> sequenceOf( CrunchProperty.String(colName, "") )
                BOOLEAN -> sequenceOf( CrunchProperty.Boolean(colName, cell.booleanCellValue) )
                ERROR -> sequenceOf( CrunchProperty.Expression(colName, "Error-${cell.errorCellValue}") )
                else -> sequenceOf( CrunchProperty.Null(colName) )
            }
        }
        return FlattenedEntrySequence(fieldsFlattened)
    }

    private fun cellAsString(cell: Cell): String {
        return when (cell.cellType) {
            _NONE -> ""
            NUMERIC -> cell.numericCellValue.toString()
            STRING -> cell.stringCellValue
            FORMULA -> cell.cellFormula.filter { it.isLetterOrDigit() }
            BLANK -> ""
            BOOLEAN -> cell.booleanCellValue.toString()
            ERROR -> "Error" + cell.errorCellValue.toString()
            else -> ""
        }
    }

    private fun deriveOutputPath(inputPath: Path): Path {
        val baseName = inputPath.fileName ?: "output"
        return inputPath.resolveSibling("$baseName.csv")
    }

}