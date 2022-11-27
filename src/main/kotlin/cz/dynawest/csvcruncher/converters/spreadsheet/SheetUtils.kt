package cz.dynawest.csvcruncher.converters.spreadsheet

import cz.dynawest.csvcruncher.CsvCruncherException
import cz.dynawest.csvcruncher.converters.EntriesNotFoundAtLocationException
import cz.dynawest.csvcruncher.converters.spreadsheet.SheetUtils.columnLettersNameToIndex
import org.apache.poi.ss.usermodel.Sheet
import org.apache.poi.ss.usermodel.Workbook
import java.nio.file.Path
import kotlin.math.max
import kotlin.math.min

object SheetUtils {

    fun computeBoundingRange(sheet: Sheet): RectRange {
        val rowsRange = sheet.firstRowNum..sheet.lastRowNum
        val colsRange = Int.MAX_VALUE..0
        val maxColsRange: IntRange = sheet.fold(colsRange) { curRange, row -> min(row.firstCellNum.toInt(), curRange.first)..max(row.lastCellNum.toInt(), curRange.last) }
        return RectRange(
            cols = maxColsRange.first..maxColsRange.last,
            rows = rowsRange.first..rowsRange.last,
        )
    }

    fun checkRangesDoOverlap(givenRange: RectRange, rectRange: RectRange) {
        if (givenRange.rows.first > rectRange.rows.last) throw EntriesNotFoundAtLocationException("The given range starts after the last row in the file. $givenRange vs. $rectRange")
        if (givenRange.rows.last < rectRange.rows.first) throw EntriesNotFoundAtLocationException("The given range ends before the first row index in the file. $givenRange vs. $rectRange")

        if (givenRange.cols.first < rectRange.cols.last) throw EntriesNotFoundAtLocationException("The given range ends before the first column index in the file. $givenRange vs. $rectRange")
        if (givenRange.cols.last < rectRange.cols.first) throw EntriesNotFoundAtLocationException("The given range ends before the first column index in the file. $givenRange vs. $rectRange")
    }

    fun columnLettersNameToIndex(letters: String) = letters[0].uppercaseChar().code - 'A'.code
    fun columnIndexToLettersName(index: Int): String {
        var index_ = index
        var colLetters = ""
        val base = 'Z'.code - 'A'.code +1
        while (index_ > 0) {
            colLetters += Char(index % base + 'A'.code).toString()
            index_ /= base
        }
        return colLetters.reversed()
    }

    fun Workbook.translateToSheetIndex(nameOrIndex: String): Int? {
        if (nameOrIndex.isBlank()) return 0
        nameOrIndex.toIntOrNull()?.let { return it }
        return this.getSheetIndex(nameOrIndex.trim()).takeIf { it >= 0 }
    }
    fun Workbook.getSheetByNameOrIndex(nameOrIndex: String): Sheet {
        val sheetIndex = nameOrIndex.let { this.translateToSheetIndex(it) ?: throw EntriesNotFoundAtLocationException("Can't find the sheet $it") }
        return this.getSheetAt(sheetIndex)
    }

}

data class RangeReference(
    val inputPath: Path?,
    val sheetNameOrIndex: String = "",
    val range: RectRange? = null,
) {
    companion object {
        fun parse(rangeRef: String): RangeReference {
            val parts = rangeRef.split('!')
            val range = RectRange.parse(parts.last())
            val sheetIndex = if (parts.size < 2) "" else parts[parts.size - 2]
            val inputPath = if (parts.size < 3) null else parts[0]
            return RangeReference(inputPath ?.let { Path.of(inputPath) }, sheetIndex, range)
        }
    }
}

data class RectRange(
    val cols: IntRange,
    val rows: IntRange,
) {
    companion object {
        fun parse(rangeString: String): RectRange? {
            if (rangeString.isBlank()) return null
            val match = rangeRegexStr.toRegex().matchEntire(rangeString.uppercase()) ?: throw CsvCruncherException("Spreadsheet range not match $rangeRegexStr: $rangeString")
            return RectRange(
                cols = columnLettersNameToIndex(match.groupValues[1]) .. columnLettersNameToIndex(match.groupValues[3]),
                rows = columnLettersNameToIndex(match.groupValues[2]) .. columnLettersNameToIndex(match.groupValues[4]),
            )
        }
        private const val rangeRegexStr = "([A-Z]+)([0-9]+):([A-Z]+)([0-9]+)"
    }
}
