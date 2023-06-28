package cz.dynawest.csvcruncher.app

import cz.dynawest.csvcruncher.CrucherConfigException

interface OptionEnum {
    val optionName: String
    val optionValue: String?

    // TODO: List<String> getOptionsAvailable();


    companion object {
        inline fun <reified T : OptionEnum> tryParseEnumOption(enumArgumentDefault: T, arg: String): T? {
            val optionIntro = "--${enumArgumentDefault.optionName}"

            if (!arg.startsWith(optionIntro))
                return null

            if (arg.endsWith(optionIntro) || arg.endsWith("=${enumArgumentDefault.optionValue}"))
                return enumArgumentDefault

            val valueStr = arg.substringAfter(optionIntro)

            val enumConstants = T::class.java.enumConstants
            return enumConstants.firstOrNull { it.optionName == valueStr }
                ?: throw CrucherConfigException("Unknown value for ${enumArgumentDefault.optionName}: $arg Try one of ${enumConstants.map { it.optionValue }}")
        }
    }

}