package cz.dynawest.csvcruncher

open class CsvCruncherException : RuntimeException {
    constructor(message: String?) : super(message) {}
    constructor(message: String?, cause: Throwable?) : super(message, cause) {}
    constructor(cause: Throwable?) : super(cause) {}
    constructor(message: String?, cause: Throwable?, enableSuppression: Boolean, writableStackTrace: Boolean) : super(message, cause, enableSuppression, writableStackTrace) {}
}

class CrucherConfigException : CsvCruncherException {
    constructor(message: String?) : super(message)
}