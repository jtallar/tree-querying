package ar.edu.itba.pod.tpe.models;

public class Tree {
    private String neighbourhood_name;
    private String street_name;
    private String common_name;
    private String diameter;

    public Tree(String neighbourhood_name, String street_name, String common_name, String diameter) {
        this.neighbourhood_name = neighbourhood_name;
        this.street_name = street_name;
        this.common_name = common_name;
        this.diameter = diameter;
    }

    @Override
    public String toString() {
        return neighbourhood_name + "--" + street_name + "--"
                            + common_name + "--" +  diameter;
    }
}
