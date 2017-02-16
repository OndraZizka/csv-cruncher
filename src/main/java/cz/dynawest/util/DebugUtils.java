package cz.dynawest.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class DebugUtils {
   public static String getEnvironmentVariablesAsString() {
      Map envVars = System.getenv();
      StringBuilder sb = new StringBuilder("Environment variables:\n");
      Iterator i$ = envVars.entrySet().iterator();

      while(i$.hasNext()) {
         Entry envVar = (Entry)i$.next();
         sb.append("  ").append((String)envVar.getKey()).append(": ").append((String)envVar.getValue()).append("\n");
      }

      return sb.toString();
   }
}
