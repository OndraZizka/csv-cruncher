package cz.dynawest.util

object ClassUtils {

    fun getCallingClassName(): String? {
        var thisClassSpotted = false;

        for (stackItem in Thread.currentThread().stackTrace) {
            if (stackItem.className == ClassUtils::class.java.name) {
                thisClassSpotted = true
                continue
            } else if (thisClassSpotted) {
                return stackItem.className
            }
        }

        return null
    }

}