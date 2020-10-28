package ar.edu.itba.pod.tpe.client.utils;

import ar.edu.itba.pod.tpe.client.exceptions.ArgumentException;

/**
 * Enum for the possible cities
 */
public enum City {
    BUE("BUE", new int[]{2, 4, 7, 11}), // COMUNA - CALLE_NOMBRE - NOMBRE_CIENTIFICO - DIAMETRO_ALTURA_PECHO
    VAN("VAN", new int[]{12, 2, 6, 15}); // NEIGHBOURHOOD_NAME - STD_STREET - COMMON_NAME - DIAMETER

    private String abbreviation;
    private int[] headersIndex;

    City(String abbreviation, int[] headersIndex) {
        this.abbreviation = abbreviation;
        this.headersIndex = headersIndex;
    }

    public String getAbbreviation() {
        return abbreviation;
    }

    public int[] getHeadersIndex() {
        return headersIndex;
    }

    public static City of(String abbreviation) throws ArgumentException {
        for (City a : values()) {
            if (a.abbreviation.equals(abbreviation))
                return a;
        }
        throw new ArgumentException("Unknown city abbreviation");
    }
}
