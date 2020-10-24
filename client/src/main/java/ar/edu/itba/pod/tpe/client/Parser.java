package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.models.Neighbourhood;
import ar.edu.itba.pod.tpe.models.Tree;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Parser {

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


    public static List<Tree> parseVANTrees(String inPath) throws IOException {
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
    }

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

}
