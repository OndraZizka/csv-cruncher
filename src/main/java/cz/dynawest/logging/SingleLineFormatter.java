package cz.dynawest.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SingleLineFormatter extends Formatter
{
    public static final FieldPosition FIELD_POSITION = new FieldPosition(1);
    //private static final String FORMAT = "{0,date} {0,time}";

    private final Date date = new Date();
    public final SimpleDateFormat SIMPLE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MessageFormat formatter;
    //private Object[] args = new Object[1];
    private static final String lineSeparator = System.lineSeparator();

    public synchronized String format(LogRecord record)
    {
        StringBuffer sb = new StringBuffer();
        this.date.setTime(record.getMillis());

        /*
        this.args[0] = this.date;
        StringBuffer sb2 = new StringBuffer();
        if (this.formatter == null) {
            this.formatter = new MessageFormat(FORMAT);
        }
        this.formatter.format(this.args, sb2, null);
        sb.append(sb2);
        */

        SIMPLE_FORMAT.format(date, sb, FIELD_POSITION);
        sb.append(" ");

        sb.append(record.getLevel().getLocalizedName());
        sb.append(" ");

        if (record.getSourceClassName() != null)
            sb.append(record.getSourceClassName());
        else
            sb.append(record.getLoggerName());

        if (record.getSourceMethodName() != null) {
            sb.append(" ");
            sb.append(record.getSourceMethodName());
        }

        sb.append(": ");
        int indent = (1000 - record.getLevel().intValue()) / 100;
        for (int i = 0; i < indent; ++i)
            sb.append(" ");

        String message = this.formatMessage(record);
        sb.append(message);
        sb.append(this.lineSeparator);
        if (record.getThrown() != null) {
            try {
                StringWriter writer = new StringWriter();
                PrintWriter pw = new PrintWriter(writer);
                record.getThrown().printStackTrace(pw);
                pw.close();
                sb.append(writer.toString());
            }
            catch (Exception ex) { }
        }

        return sb.toString();
    }
}
