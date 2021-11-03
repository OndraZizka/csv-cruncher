package cz.dynawest.util

object ClassUtils {

    fun geCurrentClassName(): String? {
        return getCallingClassName(2)
    }

    fun getCallingClassName(target: Int = 3): String? {

        var thisClassSpotted = 0;

        for (stackItem in Thread.currentThread().stackTrace) {
            if (thisClassSpotted == target) {
                return stackItem.className
            }
            else if (stackItem.className == ClassUtils::class.java.name || thisClassSpotted != 0) {
                thisClassSpotted++
                continue
            }
        }

        return null
    }

}