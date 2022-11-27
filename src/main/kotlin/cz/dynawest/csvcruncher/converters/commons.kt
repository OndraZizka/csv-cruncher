package cz.dynawest.csvcruncher.converters


open class EntriesNotFoundAtLocationException(location: String): Exception("Entries not found at the given location: $location")

val IntRange_MAX = 0..Integer.MAX_VALUE