package ar.edu.itba.pod.tpe.client;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;

public enum City {
    BUE("BUE"),
    VAN("VAN");

    private String abbreviation;

    City(String abbreviation) {
        this.abbreviation = abbreviation;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public static City of(String abbreviation) throws ArgumentException {
        for (City a : values()) {
            if (a.abbreviation.equals(abbreviation))
                return a;
        }
        throw new ArgumentException("Unknown city abbreviation");
    }
}
