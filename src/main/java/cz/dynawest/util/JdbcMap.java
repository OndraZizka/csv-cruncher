package cz.dynawest.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class JdbcMap implements Map {
   private String sTable;
   private Connection conn;
   private JdbcMap.Statements statements;

   public JdbcMap(Connection conn, String sTable) throws SQLException {
      this.conn = conn;
      this.sTable = sTable;
      this.prepareStatements();
   }

   public int size() {
      throw new UnsupportedOperationException("Not supported yet.");
   }

   public boolean isEmpty() {
      try {
         ResultSet ex = this.statements.isEmpty.executeQuery();
         ex.first();
         boolean bEmpty = ex.getInt(0) == 0;
         ex.close();
         return bEmpty;
      } catch (SQLException var3) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var3);
         throw new NullPointerException();
      }
   }

   public boolean containsKey(Object arg0) {
      throw new UnsupportedOperationException("Not supported yet.");
   }

   public boolean containsValue(Object arg0) {
      throw new UnsupportedOperationException("Not supported yet.");
   }

   public String get(Object sName) {
      try {
         this.statements.get.setString(1, (String)sName);
         ResultSet ex = this.statements.get.executeQuery();
         if(!ex.next()) {
            return null;
         } else {
            String sRet = ex.getString(0);
            ex.close();
            return sRet;
         }
      } catch (SQLException var4) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var4);
         throw new NullPointerException();
      }
   }

   public String put(String sName, String sValue) {
      try {
         if(sValue == null) {
            this.delete(sName);
         } else {
            this.statements.get.setString(1, sName);
            this.statements.get.setString(2, sValue);
            this.statements.get.executeUpdate();
         }

         return sValue;
      } catch (SQLException var4) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var4);
         throw new NullPointerException();
      }
   }

   public String remove(Object sName) {
      String sValue = this.get(sName);

      try {
         this.delete((String)sName);
         return sValue;
      } catch (SQLException var4) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var4);
         throw new NullPointerException();
      }
   }

   public void delete(String sName) throws SQLException {
      this.statements.delete.setString(1, sName);
      this.statements.get.executeUpdate();
   }

   public void putAll(Map arg0) {
      throw new UnsupportedOperationException("Not supported yet.");
   }

   public void clear() {
      try {
         this.statements.clear.executeUpdate();
      } catch (SQLException var2) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var2);
         throw new NullPointerException();
      }
   }

   public Set keySet() {
      try {
         HashSet ex = new HashSet();
         ResultSet rs = this.statements.keySet.executeQuery();

         while(rs.next()) {
            ex.add(rs.getString(1));
         }

         rs.close();
         return ex;
      } catch (SQLException var3) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var3);
         throw new NullPointerException();
      }
   }

   public Collection values() {
      try {
         HashSet ex = new HashSet();
         ResultSet rs = this.statements.values.executeQuery();

         while(rs.next()) {
            ex.add(rs.getString(1));
         }

         rs.close();
         return ex;
      } catch (SQLException var3) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var3);
         throw new NullPointerException();
      }
   }

   public Set entrySet() {
      try {
         HashSet ex = new HashSet();
         ResultSet rs = this.statements.entrySet.executeQuery();

         while(rs.next()) {
            JdbcMap.Entry entry = new JdbcMap.Entry(rs.getString(1), rs.getString(2));
            ex.add(entry);
         }

         rs.close();
         return ex;
      } catch (SQLException var4) {
         Logger.getLogger(JdbcMap.class.getName()).log(Level.SEVERE, (String)null, var4);
         throw new NullPointerException();
      }
   }

   public String getSTable() {
      return this.sTable;
   }

   private void prepareStatements() throws SQLException {
      this.statements.clear = this.conn.prepareStatement("TRUNCATE " + this.sTable + "");
      this.statements.containsKey = this.conn.prepareStatement("SELECT COUNT(*) FROM " + this.sTable + " WHERE name=?");
      this.statements.containsValue = this.conn.prepareStatement("SELECT COUNT(*) FROM " + this.sTable + " WHERE value=?");
      this.statements.entrySet = this.conn.prepareStatement("SELECT name, value FROM " + this.sTable);
      this.statements.isEmpty = this.conn.prepareStatement("SELECT COUNT(*) FROM " + this.sTable);
      this.statements.keySet = this.conn.prepareStatement("SELECT name FROM " + this.sTable);
      this.statements.delete = this.conn.prepareStatement("DELETE FROM " + this.sTable + " WHERE name=?");
      this.statements.values = this.conn.prepareStatement("SELECT value FROM " + this.sTable);
      this.statements.put = this.conn.prepareStatement("INSERT " + this.sTable + " SET name=?, value=? ON DUPLICATE KEY UPDATE `value`=VALUE(`value`)");
      this.statements.get = this.conn.prepareStatement("SELECT value FROM " + this.sTable + " WHERE name=?");
   }

   public class Entry implements java.util.Map.Entry {
      String key;
      String value;

      public Entry(String key, String value) {
         this.key = key;
         this.value = value;
      }

      public String getKey() {
         return this.key;
      }

      public String getValue() {
         return this.value;
      }

      public String setValue(String val) {
         String sTmp = this.value;
         this.value = val;
         return sTmp;
      }
   }

   class Statements {
      PreparedStatement put;
      PreparedStatement get;
      PreparedStatement isEmpty;
      PreparedStatement containsKey;
      PreparedStatement containsValue;
      PreparedStatement delete;
      PreparedStatement clear;
      PreparedStatement keySet;
      PreparedStatement values;
      PreparedStatement entrySet;
   }
}
