package cz.dynawest.util;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

public class DumpPropertiesBean {
   private String label = "Properties:";

   public void setLabel(String label) {
      this.label = label;
   }

   public void setProperties(Properties props) {
      Logger log = Logger.getLogger(this.getClass().getName());
      Set names = props.stringPropertyNames();
      log.info(this.label);

      String name;
      String val;
      for(Iterator i$ = (new TreeSet(names)).iterator(); i$.hasNext(); log.info("    " + name + ": " + val)) {
         name = (String)i$.next();
         if(name.contains("pass")) {
            val = "(hidden)";
         } else {
            val = props.getProperty(name);
         }
      }

   }
}
