package cz.dynawest.csvcruncher;

public class CsvCruncherException extends RuntimeException
{
    public CsvCruncherException(String message)
    {
        super(message);
    }

    public CsvCruncherException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public CsvCruncherException(Throwable cause)
    {
        super(cause);
    }

    public CsvCruncherException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
