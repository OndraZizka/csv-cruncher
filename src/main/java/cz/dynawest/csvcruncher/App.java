package cz.dynawest.csvcruncher;

import cz.dynawest.logging.LoggingUtils;
import cz.dynawest.logging.SystemOutHandler;
import java.util.logging.Logger;
import org.apache.commons.lang3.StringUtils;

/*
 * This was written long ago and then lost and decompiled from an old .jar of an old version, and refactored a bit.
 * So please be lenient with the code below :)
 */

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

            if (str.startsWith("--json")) {
                if (str.endsWith("=" + Options.JsonExportFormat.ARRAY.getOptionsValue()))
                    opt.jsonExportFormat = Options.JsonExportFormat.ARRAY;
                //if (str.endsWith("=" + Cruncher.JsonExportFormat.ENTRY_PER_LINE.getOptionsValue()))
                else
                    opt.jsonExportFormat = Options.JsonExportFormat.ENTRY_PER_LINE;
            }
            else if (str.startsWith("--rowNumbers")) {
                opt.initialRowNumber = -1L;
                if (str.startsWith("--rowNumbers=")) {
                    String numberStr = StringUtils.removeStart(str, "--rowNumbers=");
                    try {
                        long number = Long.parseLong(numberStr);
                        opt.initialRowNumber = number;
                    } catch (Exception ex) {
                        throw new RuntimeException("Not a valid number: " + numberStr + ". " + ex.getMessage(), ex);
                    }
                }
            }
            else if (str.startsWith("--joinInputs")) {
                // TODO
            }

            else if ("-in".equals(str)) {
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

            else if ("-v".equals(str) || "--version".equals(str)) {
                String version = Utils.getVersion();
                System.out.println(" CSV Cruncher version " + version);
                System.exit(0);
            }

            else {
                if (next != null) {
                    switch (next) {
                        case IN:
                            opt.inputPaths.add(str);
                            relPos = Math.max(relPos, 1);
                            continue;
                        case OUT:
                            opt.outputPathCsv = str;
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
                    opt.inputPaths.add(str);
                }
                else if (relPos == 1) {
                    opt.outputPathCsv = str;
                }
                else {
                    if (relPos != 2) {
                        printUsage();
                        throw new IllegalArgumentException("Wrong arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...");
                    }

                    opt.sql = str;
                }
            }
        }

        if (!opt.isFilled()) {
            printUsage();
            throw new IllegalArgumentException("Not enough arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...");
        }
        else {
            return opt;
        }
    }

    private static void printUsage()
    {
        System.out.println("  Usage:");
        System.out.println("    crunch [-in] <inCSV> [<inCSV> ...] [-out] <outCSV> [--<option> --...] [-sql] <SQL>");
    }

    private enum OptionsNext
    {
        IN,
        OUT,
        SQL,
        DBPATH;
    }

}
