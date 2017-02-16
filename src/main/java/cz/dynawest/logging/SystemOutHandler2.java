package cz.dynawest.logging;

import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

public class SystemOutHandler2 extends StreamHandler {
   private void configure() {
      LogManager manager = LogManager.getLogManager();
      String cname = this.getClass().getName();

      Level level;
      try {
         level = Level.parse(manager.getProperty(cname + ".level"));
      } catch (Exception var8) {
         level = Level.INFO;
      }

      this.setLevel(level);
      this.setFormatter(this.getFormatterProperty(cname + ".formatter", new SimpleFormatter()));

      try {
         this.setEncoding(manager.getProperty(cname + ".encoding"));
      } catch (Exception var7) {
         try {
            this.setEncoding((String)null);
         } catch (Exception var6) {
            ;
         }
      }

   }

   Formatter getFormatterProperty(String name, Formatter defaultValue) {
      LogManager manager = LogManager.getLogManager();
      String val = manager.getProperty(name);

      try {
         if(val != null) {
            Class ex = ClassLoader.getSystemClassLoader().loadClass(val);
            return (Formatter)ex.newInstance();
         }
      } catch (Exception var6) {
         ;
      }

      return defaultValue;
   }

   public SystemOutHandler2() {
      this.configure();
      this.setOutputStream(System.err);
   }

   public void publish(LogRecord record) {
      super.publish(record);
      this.flush();
   }

   public void close() {
      this.flush();
   }
}
