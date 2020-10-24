package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;
import org.omg.CORBA.COMM_FAILURE;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class Parser {

    private static final String TREES_FILE_PREFIX = "arboles";
    private static final String NEIGHBOURHOODS_FILE_PREFIX = "barrios";
    private static final String FILE_EXTENSION = ".csv";

    /** BUE **/
    private static final int COMUNA = 2;
    private static final int CALLE_NOMBRE = 4;
    private static final int NOMBRE_CIENTIFICO = 7;
    private static final int DIAMETRO_ALTURA_PECHO = 11;

    /** VAN **/
    private static final int STD_STREET = 2;
    private static final int COMMON_NAME = 6;
    private static final int NEIGHBOURHOOD_NAME = 12;
    private static final int DIAMETER = 15;

    // directoryPath termina en / . Eg: /home/julian/Desktop/POD/tpe2-g6/test-files/
    public static Map<Neighbourhood, List<Tree>> parseTrees(String directoryPath, City city) throws IOException {
        int[] headersIndex = getHeaderNumbers(city);

        Map<Neighbourhood, List<Tree>> trees = new HashMap<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(directoryPath + TREES_FILE_PREFIX + city.getAbbreviation() + FILE_EXTENSION), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            Neighbourhood neighbourhood = new Neighbourhood(parse[headersIndex[0]]);
            trees.computeIfAbsent(neighbourhood, k -> new ArrayList<>());
            trees.get(neighbourhood).add(new Tree(parse[headersIndex[1]], parse[headersIndex[2]], Double.valueOf(parse[headersIndex[3]])));
        }

        return trees;
    }

    public static Map<String, Integer> parseNeighbourhood(String directoryPath, City city) throws IOException{
        Map<String, Integer> neighbourhoods = new HashMap<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(directoryPath + NEIGHBOURHOODS_FILE_PREFIX + city.getAbbreviation() + FILE_EXTENSION)).
                stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            neighbourhoods.put(parse[0], Integer.valueOf(parse[1]));
        }

        return neighbourhoods;
    }

    private static int[] getHeaderNumbers(City city) {
        switch (city) {
            case BUE:
                return new int[]{COMUNA, CALLE_NOMBRE, NOMBRE_CIENTIFICO, DIAMETRO_ALTURA_PECHO};
            case VAN:
                return new int[]{NEIGHBOURHOOD_NAME, STD_STREET, COMMON_NAME, DIAMETER};
            default:
                throw new IllegalArgumentException("City not found");
        }
    }
}
