package cz.dynawest.util;

import java.util.Iterator;

public class IteratorStringBuilder {
   public static String create(Iterable iterable, IteratorStringBuilder.Stringer stringer) {
      if(null != iterable && iterable.iterator().hasNext()) {
         String delimiter = ", ";
         StringBuilder sb = new StringBuilder();
         Iterator res = iterable.iterator();

         while(res.hasNext()) {
            Object item = res.next();
            sb.append(delimiter);
            sb.append(stringer.makeString(item));
         }

         String res1 = sb.toString().substring(2);
         return res1;
      } else {
         return "";
      }
   }

   public interface Stringer {
      IteratorStringBuilder.Stringer className = new IteratorStringBuilder.Stringer() {
         public String makeString(Object toBeStringed) {
            return toBeStringed.getClass().getName();
         }
      };

      String makeString(Object var1);
   }
}
