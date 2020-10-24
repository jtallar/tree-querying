package ar.edu.itba.pod.tpe.client;

public enum City {
    BUE("BUE"),
    VAN("VAN");

    private String abbreviation;

    City(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public static City of(String abbreviation) {
        for (City a : values()) {
            if (a.abbreviation.equals(abbreviation))
                return a;
        }
        throw new IllegalArgumentException("No city found");
    }
}
