package cz.dynawest.logging;

import java.util.logging.ConsoleHandler;

public class SystemOutHandler extends ConsoleHandler {
   public SystemOutHandler() {
      this.setOutputStream(System.out);
   }
}
