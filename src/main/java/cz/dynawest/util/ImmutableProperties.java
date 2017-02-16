package cz.dynawest.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.InvalidPropertiesFormatException;
import java.util.Map;
import java.util.Properties;

public class ImmutableProperties extends Properties {
   public ImmutableProperties(Properties original) {
      super.putAll(original);
   }

   public Object setProperty(String key, String val) {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized void load(Reader arg0) throws IOException {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized void load(InputStream arg0) throws IOException {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized void loadFromXML(InputStream arg0) throws IOException, InvalidPropertiesFormatException {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized void clear() {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized Object put(Object arg0, Object arg1) {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized void putAll(Map arg0) {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }

   public synchronized Object remove(Object arg0) {
      throw new UnsupportedOperationException("Immutable Properties object - can\'t be changed.");
   }
}
