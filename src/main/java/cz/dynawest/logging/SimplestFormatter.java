package cz.dynawest.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;

public class SimplestFormatter extends Formatter {
   Date dat = new Date();
   private static final String format = "{0,date} {0,time}";
   private MessageFormat formatter;
   private Object[] args = new Object[1];
   private String lineSeparator = "\n";

   public synchronized String format(LogRecord record) {
      StringBuffer sb = new StringBuffer();
      Level level = record.getLevel();
      if(level == Level.WARNING) {
         sb.append("Varov�n�: ");
      } else if(level == Level.SEVERE) {
         sb.append("Chyba!    ");
      } else {
         sb.append("          ");
      }

      String message = this.formatMessage(record);
      sb.append(message);
      sb.append(this.lineSeparator);
      if(record.getThrown() != null) {
         try {
            StringWriter ex = new StringWriter();
            PrintWriter pw = new PrintWriter(ex);
            record.getThrown().printStackTrace(pw);
            pw.close();
            sb.append(ex.toString());
         } catch (Exception var7) {
            ;
         }
      }

      return sb.toString();
   }
}
