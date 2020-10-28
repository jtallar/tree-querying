package ar.edu.itba.pod.tpe.client.utils;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;

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

    /**
     * Reads the trees files for the given city and converts it to a list
     * @param directoryPath Directory where the files are (ends in /)
     * @param city The corresponding city
     * @return The list of trees
     * @throws IOException
     */
    public static List<Tree> parseTrees(String directoryPath, City city) throws IOException {
        /** get file header indexes */
        int[] headersIndex = city.getHeadersIndex();

        List<Tree> trees = new ArrayList<>();

        /** delete header */
        List<String> file = Files.readAllLines(Paths.get(directoryPath + TREES_FILE_PREFIX + city.getAbbreviation() + FILE_EXTENSION), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        /** analyze each line */
        file.forEach(line -> {
            String[] parse = line.split(";");
            Tree tree = new Tree(parse[headersIndex[0]], parse[headersIndex[1]], parse[headersIndex[2]], Double.parseDouble(parse[headersIndex[3]]));
            trees.add(tree);
        });
        return trees;
    }


    /**
     * Reads the neighbourhood files for the given city and converts it to a map
     * @param directoryPath Directory where the files are (ends in /)
     * @param city The corresponding city
     * @return The map with the neighbourhoods and its population
     * @throws IOException
     */
    public static Map<String, Long> parseNeighbourhood(String directoryPath, City city) throws IOException{
        Map<String, Long> neighbourhoods = new HashMap<>();

        /** delete header */
        List<String> file = Files.readAllLines(Paths.get(directoryPath + NEIGHBOURHOODS_FILE_PREFIX + city.getAbbreviation() + FILE_EXTENSION), StandardCharsets.ISO_8859_1)
                .stream().skip(1).collect(Collectors.toList());

        /** analyze each line */
        file.forEach(line -> {
            String[] parse = line.split(";");
            neighbourhoods.put(parse[0], Long.valueOf(parse[1]));
        });
        return neighbourhoods;
    }
}
