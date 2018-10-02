package cz.dynawest.csvcruncher;

import cz.dynawest.logging.LoggingUtils;
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
            String arg = args[i];
            System.out.println("XXX " + arg);

            // JSON output
            if (arg.startsWith("--json")) {
                if (arg.endsWith("=" + Options.JsonExportFormat.ARRAY.getOptionValue()))
                    opt.jsonExportFormat = Options.JsonExportFormat.ARRAY;
                else
                    opt.jsonExportFormat = Options.JsonExportFormat.ENTRY_PER_LINE;
            }

            // Row numbers
            else if (arg.startsWith("--rowNumbers")) {
                opt.initialRowNumber = -1L;
                if (arg.startsWith("--rowNumbers=")) {
                    String numberStr = StringUtils.removeStart(arg, "--rowNumbers=");
                    try {
                        long number = Long.parseLong(numberStr);
                        opt.initialRowNumber = number;
                    } catch (Exception ex) {
                        throw new RuntimeException("Not a valid number: " + numberStr + ". " + ex.getMessage(), ex);
                    }
                }
            }

            // Combine input files
            else if (arg.startsWith("--" + Options.CombineInputFiles.OPTION)) {
                if (arg.endsWith("=" + Options.CombineInputFiles.INTERSECT.getOptionValue()))
                    opt.combineInputFiles = Options.CombineInputFiles.INTERSECT;
                else if (arg.endsWith("=" + Options.CombineInputFiles.EXCEPT.getOptionValue()))
                    opt.combineInputFiles = Options.CombineInputFiles.EXCEPT;
                else
                    opt.combineInputFiles = Options.CombineInputFiles.CONCAT;
            }

            // Combine files in input directories
            else if (arg.startsWith("--" + Options.CombineDirectories.OPTION)) {
                if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_EACH_DIR.getCombineMode()))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_EACH_DIR;
                else if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR.getCombineMode()))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_INPUT_SUBDIR;
                else if (arg.endsWith("=" + Options.CombineDirectories.COMBINE_PER_INPUT_DIR.getCombineMode()))
                    opt.combineDirs = Options.CombineDirectories.COMBINE_PER_INPUT_DIR;
                else
                    opt.combineDirs = Options.CombineDirectories.COMBINE_ALL_FILES;
            }

            else if ("-in".equals(arg)) {
                next = App.OptionsNext.IN;
            }
            else if ("-out".equals(arg)) {
                next = App.OptionsNext.OUT;
                relPos = 2;
            }
            else if ("-sql".equals(arg)) {
                next = App.OptionsNext.SQL;
                relPos = 3;
            }
            else if ("-db".equals(arg)) {
                next = App.OptionsNext.DBPATH;
            }

            else if ("-v".equals(arg) || "--version".equals(arg)) {
                String version = Utils.getVersion();
                System.out.println(" CSV Cruncher version " + version);
                System.exit(0);
            }

            else if (arg.startsWith("-")) {
                System.out.println("ERROR: Unknown parameter: " + arg);
                System.exit(1);
            }

            else {
                if (next != null) {
                    switch (next) {
                        case IN:
                            opt.inputPaths.add(arg);
                            relPos = Math.max(relPos, 1);
                            continue;
                        case OUT:
                            opt.outputPathCsv = arg;
                            relPos = Math.max(relPos, 2);
                            continue;
                        case SQL:
                            opt.sql = arg;
                            relPos = Math.max(relPos, 3);
                            continue;
                        case DBPATH:
                            opt.dbPath = arg;
                            continue;
                        default:
                            next = null;
                    }
                }

                ++relPos;
                if (relPos == 0) {
                    opt.inputPaths.add(arg);
                }
                else if (relPos == 1) {
                    opt.outputPathCsv = arg;
                }
                else {
                    if (relPos != 2) {
                        printUsage();
                        throw new IllegalArgumentException("Wrong arguments. Usage: crunch [-in] <inCSV> [-out] <outCSV> [-sql] <SQL> ...");
                    }

                    opt.sql = arg;
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
