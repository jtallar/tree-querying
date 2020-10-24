package ar.edu.itba.pod.tpe.models;

public class Neighbourhood {

    private String name;
    private Integer population;

    public Neighbourhood(String name) {
        this.name = name;
        this.population = 0;
    }

    public Neighbourhood(String name, Integer population) {
        this.name = name;
        this.population = population;
    }

    @Override
    public String toString() {
        return   name + "--" + population;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Neighbourhood)) return false;
        Neighbourhood that = (Neighbourhood) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }
}
