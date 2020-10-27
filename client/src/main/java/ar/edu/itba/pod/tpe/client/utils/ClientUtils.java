package ar.edu.itba.pod.tpe.client.utils;

import ar.edu.itba.pod.tpe.utils.ComparablePair;
import ar.edu.itba.pod.tpe.utils.ComparableTrio;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedSet;

public class ClientUtils {
    public static InetSocketAddress getInetAddress(String hostPort) throws URISyntaxException {
        if (hostPort == null) throw new URISyntaxException("", "URI can't be null");
        URI uri = new URI("my://" + hostPort);
        if (uri.getHost() == null || uri.getPort() == -1) {
            throw new URISyntaxException(uri.toString(), "URI must have host and port parts");
        }
        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    /**
     * Opens the csv printer, executes the printing function, and closes the printer.
     * @param file File path for the csv printer.
     * @param result Result from the service response.
     * @param printFunction Print function to be applied.
     */
    public static void genericCSVPrinter(String file, SortedSet<ComparablePair<String, String>> result, ThrowableBiConsumer<SortedSet<ComparablePair<String, String>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }

    /**
     * Opens the csv printer, executes the printing function, and closes the printer.
     * @param file File path for the csv printer.
     * @param result Result from the service response.
     * @param printFunction Print function to be applied.
     */
    public static void genericCSVPrinter4(String file, SortedSet<ComparablePair<Double, String>> result, ThrowableBiConsumer<SortedSet<ComparablePair<Double, String>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }


    // TODO esto despues mejorarlo, ver de hacer generico

    /**
     * Opens the csv printer, executes the printing function, and closes the printer.
     * @param file File path for the csv printer.
     * @param result Result from the service response.
     * @param printFunction Print function to be applied.
     */
    public static void genericCSVPrinter2(String file, Set<ComparablePair<Double, String>> result, ThrowableBiConsumer<Set<ComparablePair<Double, String>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }

    /**
     * Opens the csv printer, executes the printing function, and closes the printer.
     * @param file File path for the csv printer.
     * @param result Result from the service response.
     * @param printFunction Print function to be applied.
     */
    public static void genericCSVPrinter3(String file, SortedSet<ComparableTrio<Long, String, String>> result, ThrowableBiConsumer<SortedSet<ComparableTrio<Long, String, String>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }


    public static void mapSVPrinter(String file, Map<String,ComparablePair<String,Long>> result, ThrowableBiConsumer< Map<String,ComparablePair<String,Long>>, CSVPrinter, IOException> printFunction) {
        try (final CSVPrinter csvPrinter = new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';')
                .withRecordSeparator('\n'))) {

            // Applies function to result and writes on the csv
            printFunction.accept(result, csvPrinter);

            // Closes the csv printer
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }
}
