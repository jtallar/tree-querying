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
    /*public static Map<Neighbourhood, List<Tree>> parseTreesMapWithNeighbours(String directoryPath, City city, boolean withPopulation) throws IOException {
        int[] headersIndex = getHeaderNumbers(city);

        Map<String, Integer> neighbourhoods = new HashMap<>();
        if (withPopulation) neighbourhoods = parseNeighbourhoodMap(directoryPath, city);

        Map<Neighbourhood, List<Tree>> trees = new HashMap<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(directoryPath + TREES_FILE_PREFIX + city.getAbbreviation() + FILE_EXTENSION), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            String neighbourhoodName = parse[headersIndex[0]];

            if (withPopulation) {
                Optional.ofNullable(neighbourhoods.get(neighbourhoodName)).ifPresent(population -> {
                    Neighbourhood neighbourhood = new Neighbourhood(neighbourhoodName, population);
                    trees.computeIfAbsent(neighbourhood, k -> new ArrayList<>());
                    trees.get(neighbourhood).add(new Tree(parse[headersIndex[1]], parse[headersIndex[2]], Double.valueOf(parse[headersIndex[3]])));
                });
            } else {
                Neighbourhood neighbourhood = new Neighbourhood(neighbourhoodName);
                trees.computeIfAbsent(neighbourhood, k -> new ArrayList<>());
                trees.get(neighbourhood).add(new Tree(parse[headersIndex[1]], parse[headersIndex[2]], Double.valueOf(parse[headersIndex[3]])));
            }
        }

        return trees;
    }*/

    // directoryPath termina en / . Eg: /home/julian/Desktop/POD/tpe2-g6/test-files/
    // FIXME> ESTE Y EL DE ABAJO SERIAN LOS QUE QUEDEN SI FUNCA LO DEL KEYPREDICATE
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

/*
    public static List<Neighbourhood> parseNeighbourhood(String inPath) throws IOException{
        List<Neighbourhood> neighbourhoods = new ArrayList<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(inPath)).
                stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            neighbourhoods.add(new Neighbourhood(parse[0], Integer.valueOf(parse[1])));
        }

        return neighbourhoods;
    }

    public static List<Tree> parseTrees(String inPath, City city) throws IOException {
        int[] headersIndex = getHeaderNumbers(city);

        List<Tree> trees = new ArrayList<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(inPath), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            trees.add(new Tree(parse[headersIndex[0]], parse[headersIndex[1]],
                    parse[headersIndex[2]], Double.valueOf(parse[headersIndex[3]])));
        }

        return trees;
    }*/

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

/*    public static List<Tree> parseVANTrees(String inPath) throws IOException {
        List<Tree> trees = new ArrayList<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(inPath))
                .stream().skip(1).collect(Collectors.toList());


        for(String line : file ) {
            String[] parse = line.split(";");
            trees.add(new Tree(parse[NEIGHBOURHOOD_NAME], parse[STD_STREET],
                        parse[COMMON_NAME], parse[DIAMETER]));
        }

        return trees;
    }

    public static List<Tree> parseBUETrees(String inPath) throws IOException {
        List<Tree> trees = new ArrayList<>();
        //Elimino el header
        List<String> file = Files.readAllLines(Paths.get(inPath), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        for(String line : file ) {
            String[] parse = line.split(";");
            trees.add(new Tree(parse[COMUNA], parse[CALLE_NOMBRE],
                    parse[NOMBRE_CIENTIFICO], parse[DIAMETRO_ALTURA_PECHO]));
        }

        return trees;
    }*/

}
