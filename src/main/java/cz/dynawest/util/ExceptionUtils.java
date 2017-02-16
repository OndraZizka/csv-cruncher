package cz.dynawest.util;

public class ExceptionUtils {
   public static Throwable getRootCause(Throwable ex) {
      int i = 0;

      Throwable next;
      while(null != (next = ex.getCause())) {
         ex = next;
         ++i;
         if(i > 100) {
            break;
         }
      }

      return ex;
   }

   public static boolean hasCause(Throwable ex, Class cls) {
      int i = 0;

      while(null != ex) {
         if(cls.isInstance(ex)) {
            return true;
         }

         ex = ex.getCause();
         ++i;
         if(i > 100) {
            break;
         }
      }

      return false;
   }
}
