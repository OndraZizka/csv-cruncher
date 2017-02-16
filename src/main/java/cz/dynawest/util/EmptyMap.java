package cz.dynawest.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class EmptyMap implements Map {
   public static final EmptyMap INSTANCE = new EmptyMap();

   public int size() {
      return 0;
   }

   public boolean isEmpty() {
      return true;
   }

   public boolean containsKey(Object arg0) {
      return false;
   }

   public boolean containsValue(Object arg0) {
      return false;
   }

   public Object get(Object arg0) {
      return null;
   }

   public Object put(Object arg0, Object arg1) {
      throw new UnsupportedOperationException("This is EmptyMap.");
   }

   public Object remove(Object arg0) {
      return null;
   }

   public void putAll(Map arg0) {
      throw new UnsupportedOperationException("This is EmptyMap.");
   }

   public void clear() {
   }

   public Set keySet() {
      return new HashSet(0);
   }

   public Collection values() {
      return new ArrayList(0);
   }

   public Set entrySet() {
      return new HashSet(0);
   }
}
