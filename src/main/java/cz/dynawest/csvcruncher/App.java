package cz.dynawest.csvcruncher;

import cz.dynawest.logging.LoggingUtils;
import java.util.logging.Logger;
import cz.dynawest.csvcruncher.Cruncher;
import cz.dynawest.csvcruncher.Cruncher.Options;

public class App
{
    public static final String STR_USAGE = "Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL>";
    private static final Logger log = Logger.getLogger(App.class.getName());

    public static void main(String[] args) throws Exception
    {
        LoggingUtils.initLogging();

        try {
            Options options = parseArgs(args);
            (new Cruncher(options)).crunch();
        }
        catch (IllegalArgumentException var3) {
            System.out.println("" + var3.getMessage());
            System.exit(1);
        }

    }

    private static Options parseArgs(String[] args)
    {
        Options opt = new Options();
        int relPos = -1;
        App.OptionsNext next = null;

        for (int i = 0; i < args.length; ++i) {
            String str = args[i];
            if ("-in".equals(str)) {
                next = App.OptionsNext.IN;
            }
            else if ("-out".equals(str)) {
                next = App.OptionsNext.OUT;
                relPos = 2;
            }
            else if ("-sql".equals(str)) {
                next = App.OptionsNext.SQL;
                relPos = 3;
            }
            else if ("-db".equals(str)) {
                next = App.OptionsNext.DBPATH;
            }
            else {
                if (next != null) {
                    switch (next) {
                        case IN:
                            opt.csvPathIn = str;
                            relPos = Math.max(relPos, 1);
                            continue;
                        case OUT:
                            opt.csvPathOut = str;
                            relPos = Math.max(relPos, 2);
                            continue;
                        case SQL:
                            opt.sql = str;
                            relPos = Math.max(relPos, 3);
                            continue;
                        case DBPATH:
                            opt.dbPath = str;
                            continue;
                        default:
                            next = null;
                    }
                }

                ++relPos;
                if (relPos == 0) {
                    opt.csvPathIn = str;
                }
                else if (relPos == 1) {
                    opt.csvPathOut = str;
                }
                else {
                    if (relPos != 2) {
                        throw new IllegalArgumentException("Wrong arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL>");
                    }

                    opt.sql = str;
                }
            }
        }

        if (!opt.isFilled()) {
            throw new IllegalArgumentException("Not enough arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL>");
        }
        else {
            return opt;
        }
    }

    private static void printUsage()
    {
        System.out.println("  Usage:");
        System.out.println("    crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL>");
    }

    private enum OptionsNext
    {
        IN,
        OUT,
        SQL,
        DBPATH;
    }

}
