package org.corfudb.universe;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.corfudb.universe.dynamic.Dynamic;

import java.time.Duration;


@Slf4j
public class LongevityApp {
    private static final String TIME_UNIT = "time_unit";
    private static final String TIME_AMOUNT = "time_amount";

    public static void main(String[] args) {
        Options options = new Options();

        Option amountTime = new Option("t", TIME_AMOUNT, true, "time amount");
        amountTime.setRequired(true);
        Option timeUnit = new Option("u", TIME_UNIT, true, "time unit (s, m, h)");
        timeUnit.setRequired(true);

        options.addOption(amountTime);
        options.addOption(timeUnit);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
            String timeUnitValue = cmd.getOptionValue(TIME_UNIT);
            if (!timeUnitValue.equals("m") &&
                    !timeUnitValue.equals("s") &&
                    !timeUnitValue.equals("h")){
                throw new ParseException("Time unit should be {s,m,h}");
            }
        } catch (ParseException e) {
            log.info(e.getMessage());
            formatter.printHelp("longevity", options);

            System.exit(1);
            return;
        }


        long longevity;
        long amountTimeValue = Long.parseLong(cmd.getOptionValue(TIME_AMOUNT));
        String timeUnitValue = cmd.getOptionValue(TIME_UNIT);
        switch (timeUnitValue) {
            case "s":
                longevity = Duration.ofSeconds(amountTimeValue).toMillis();
                break;
            case "m":
                longevity = Duration.ofMinutes(amountTimeValue).toMillis();
                break;
            case "h":
                longevity = Duration.ofHours(amountTimeValue).toMillis();
                break;
            default:
                longevity = Duration.ofHours(1).toMillis();
        }

        Dynamic universeDynamic = new Dynamic.PseudoRandomDynamic(longevity);
        try{
            universeDynamic.initialize();
            universeDynamic.run();
            log.info("Longevity Test finished!!!");
            //TODO: send report to a spreadsheet in the column Dynamic.REPORT_FIELD_EVENT
        }
        catch (Exception ex){
            log.error("Unexpected error running a universe dynamic.", ex);
            //TODO: send report to a spreadsheet in the column Dynamic.REPORT_FIELD_EVENT
        }
        finally {
            universeDynamic.shutdown();
        }
    }
}
