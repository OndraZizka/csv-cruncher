package cz.dynawest.util;

public final class StringTools {
   public static String escapeJPAQL(String value, String escapeChar) {
      value = value.replace("%", escapeChar + "%");
      value = value.replace("_", escapeChar + "_");
      value = value.replace(escapeChar, escapeChar + escapeChar);
      return value;
   }

   public static String escapeJPAQL(String value) {
      return escapeJPAQL(value, "\\");
   }
}
