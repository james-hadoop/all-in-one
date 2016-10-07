package com.james.common.util;

import java.text.ParseException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

public class CommandLineUtil {
    /**
     * Parse the command line parameters.
     *
     * @param args
     *            The parameters to parse.
     * @return The parsed command line.
     * @throws org.apache.commons.cli.ParseException
     *             When the parsing of the parameters fails.
     */
    public static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();

        // add "-i" input option
        Option o = new Option("i", "input", true, "input file to read from (must exist)");
        o.setArgName("input-file");
        o.setRequired(true);
        options.addOption(o);

        // add "-o" output option
        o = new Option("o", "output", true, "output file to write from (must exist)");
        o.setArgName("output-file");
        options.addOption(o);
        o.setRequired(true);
        options.addOption(o);

        options.addOption("d", "debug", false, "switch on DEBUG log level");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            System.err.println("ERROR: " + e.getMessage() + "\n");
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(" ", options, true);
            System.exit(-1);
        }
        if (cmd.hasOption("d")) {
            System.out.println("DEBUG ON");
        }
        return cmd;
    }
}