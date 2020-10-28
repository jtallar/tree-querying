package ar.edu.itba.pod.tpe.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * Serializable Data class modeling the street information.
 */
public class Street implements DataSerializable {

    private String neighbourhood;
    private String street;

    /** Constructors */

    public Street() {
        /** Empty constructor for Hazelcast */
    }

    public Street(String neighbourhood, String street) {
        this.neighbourhood = neighbourhood;
        this.street = street;
    }

    /** Getters */

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public String getStreet() {
        return street;
    }

    /**
     * Writes the data from a street into a {@link ObjectDataOutput}
     * @param out the object data to be saved serialized into
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(neighbourhood);
        out.writeUTF(street);
    }

    /**
     * Reads the data from a {@link ObjectDataInput} and converts to a street
     * @param in the object data to be deserialized
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        neighbourhood = in.readUTF();
        street = in.readUTF();
    }

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     * @param o the {@link Street} to which this one is to be checked for equality
     * @return true if the underlying objects of the Street are both considered equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Street that = (Street) o;
        return neighbourhood.equals(that.neighbourhood) &&
               street.equals(that.street);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     * @return a hashcode of the Street
     */
    @Override
    public int hashCode() {
        return (neighbourhood == null ? 0 : neighbourhood.hashCode()) ^
                (street == null ? 0 : street.hashCode());
    }

    /**
     * Simply overrides the string printing for this class
     * @return the resulting print string
     */
    @Override
    public String toString() {
        return "Street{" +
                "neighbourhood='" + neighbourhood + '\'' +
                ", street='" + street + '\'' +
                '}';
    }
}
