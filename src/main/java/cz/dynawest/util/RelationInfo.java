package cz.dynawest.util;

public class RelationInfo {
   RelationInfo.Type type;
   String description = "";
   String parentTable;
   String parentId;
   String childTable;
   String childId;
   String childFK;

   public RelationInfo(RelationInfo.Type type, String description, String parentTable, String parentId, String childTable, String childId, String childFK) {
      this.type = type;
      this.description = description;
      this.parentTable = parentTable;
      this.parentId = parentId;
      this.childTable = childTable;
      this.childId = childId;
      this.childFK = childFK;
   }

   public RelationInfo(RelationInfo.Type type, String parentTable, String parentId, String childTable, String childId, String childFK) {
      this.type = type;
      this.parentTable = parentTable;
      this.parentId = parentId;
      this.childTable = childTable;
      this.childId = childId;
      this.childFK = childFK;
   }

   public String getChildFK() {
      return this.childFK;
   }

   public String getChildId() {
      return this.childId;
   }

   public String getChildTable() {
      return this.childTable;
   }

   public String getDescription() {
      return this.description;
   }

   public String getParentId() {
      return this.parentId;
   }

   public String getParentTable() {
      return this.parentTable;
   }

   public RelationInfo.Type getType() {
      return this.type;
   }

   public static enum Type {
      REL_1_1,
      REL_1_01,
      REL_1_0N,
      REL_1_1N;
   }
}
