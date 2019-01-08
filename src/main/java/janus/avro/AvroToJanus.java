package janus.avro;

import org.apache.avro.Schema;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class AvroToJanus {
    private static final Logger LOGGER = LoggerFactory.getLogger(AvroToJanus.class);
    private static Options options;
    static {
        options = new Options();
        options.addOption("c", "configuration", true, "Specify the JanusGraph Configuration File.");
        options.addOption("s", "schema", true, "Specify the Avro schema.");
        options.addOption("d", "debug", false, "Enable debugging output.");
    }

    private static CommandLine parseArguments(String[] args) throws Exception {
        CommandLineParser parser = new BasicParser();
        return parser.parse(options, args);
    }

    private static Schema getSchema(String schema) throws IOException {
        File file = new File(schema);
        if (!file.exists()) {
            throw new FileNotFoundException(schema);
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IOUtils.copy(new FileInputStream(file), out);
        out.close();

        return new Schema.Parser().parse(new String(out.toByteArray()));
    }

    public static void main(String[] args) throws Exception {
        CommandLine arguments = parseArguments(args);

        if (!arguments.hasOption("c") && !arguments.hasOption("s")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Main", options);
            System.exit(1);
        }

        Schema schema = null;
        try {
            schema = getSchema(arguments.getOptionValue("s"));
            if (arguments.hasOption("d")) {
                LOGGER.debug(String.format("Loaded %s", arguments.getOptionValue("s")));
                LOGGER.debug("Discovered the following root fields:");
                for (Schema.Field field : schema.getFields()) {
                    LOGGER.debug(String.format("\t* Name: %s; Type: %s", field.name(), field.schema().getType().getName()));
                }
            }
        } catch (IOException ex) {
            System.out.println("Could not load schema.");
            System.exit(1);
        }


    }
}
