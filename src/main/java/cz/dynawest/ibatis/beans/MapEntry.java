package cz.dynawest.ibatis.beans;

public class MapEntry {
   String table;
   String name;
   String value;

   public MapEntry(String name, String value) {
      this.name = name;
      this.value = value;
   }

   public MapEntry(String table, String name, String value) {
      this.table = table;
      this.name = name;
      this.value = value;
   }
}
