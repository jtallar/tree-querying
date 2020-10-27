package ar.edu.itba.pod.tpe.client.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

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
     * @param header The CSV header to be printed.
     */
    public static <K, V, S extends Map<K, V>> void genericMapCSVPrinter(String file, S result, ThrowableBiConsumer<Map.Entry<K, V>, CSVPrinter, IOException> printFunction, String header) {
        try (final CSVPrinter csvPrinter =
                     new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';').withRecordSeparator('\n'))) {

            /** prints csv header */
            csvPrinter.printRecord(header);

            /** iterates over the entry set and accepts print function */
            for (Map.Entry<K, V> e : result.entrySet()) printFunction.accept(e, csvPrinter);

            /** closes the csv printer */
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
     * @param header The CSV header to be printed.
     */
    public static <R, S extends Set<R>> void genericSetCSVPrinter(String file, S result, ThrowableBiConsumer<R, CSVPrinter, IOException> printFunction, String header) {
        try (final CSVPrinter csvPrinter =
                     new CSVPrinter(new FileWriter(file), CSVFormat.newFormat(';').withRecordSeparator('\n'))) {

            /** prints csv header */
            csvPrinter.printRecord(header);

            /** iterates over the set and accepts print function */
            for (R r : result) printFunction.accept(r, csvPrinter);

            /** closes the csv printer */
            csvPrinter.flush();
        } catch (IOException e){
            System.err.println("Error while printing CSV file");
        }
    }

}
