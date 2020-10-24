package ar.edu.itba.pod.tpe.models;

public class Neighbourhood {

    private String name;
    private Integer population;

    public Neighbourhood(String name, Integer population) {
        this.name = name;
        this.population = population;
    }


    @Override
    public String toString() {
        return   name + "--" + population;
    }
}
