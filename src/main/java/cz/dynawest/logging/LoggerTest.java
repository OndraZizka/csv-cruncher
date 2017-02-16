package cz.dynawest.logging;

import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

public class LoggerTest {
   public void Test() {
      Logger log = Logger.getLogger("ZpracujArchiv");
      log.setUseParentHandlers(false);
      StreamHandler h = new StreamHandler(System.out, new SimpleFormatter());
      h.setLevel(Level.ALL);
      log.addHandler(h);

      try {
         FileHandler h1 = new FileHandler("Insolvence.log", '썐', 1);
         h1.setLevel(Level.ALL);
         h1.setFormatter(new SimpleFormatter());
         log.addHandler(h1);
      } catch (Exception var4) {
         var4.printStackTrace();
      }

      log.setLevel(Level.ALL);
      log.entering("Main", "ZpracujArchiv");
      log.severe("Test SEVERE");
      log.info("Test INFO");
      log.log(Level.FINE, "Test FINE");
   }

   public void TestConfig() {
      System.setProperty("java.util.logging.config.file", "logging.properties");

      try {
         LogManager.getLogManager().readConfiguration();
      } catch (Exception var2) {
         var2.printStackTrace();
      }

      Logger.getLogger("").severe("Root SEVERE");
      Logger.getLogger("").warning("Root WARN");
      Logger.getLogger("Foo").info("Foo INFO");
      Logger.getLogger("Foo").warning("Foo WARN");
      Logger.getLogger("Foo.Aj").info("Foo.Aj INFO");
      Logger.getLogger("Foo.Aj").warning("Foo.Aj WARN");
   }

   public void TestSlf4j() {
   }
}
