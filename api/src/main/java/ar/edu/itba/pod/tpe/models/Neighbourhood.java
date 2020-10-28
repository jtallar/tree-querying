package ar.edu.itba.pod.tpe.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

/**
 * Serializable Data class modeling the neighbourhood information.
 */
public class Neighbourhood implements DataSerializable {

    private String name;

    /** Constructors */

    public Neighbourhood() {
        /** Empty constructor for Hazelcast */
    }

    public Neighbourhood(String name) {
        this.name = name;
    }

    /** Getters */

    public String getName() {
        return name;
    }

    /**
     * Writes the data from a neighbourhood into a {@link ObjectDataOutput}
     * @param out the object data to be saved serialized into
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
    }

    /**
     * Reads the data from a {@link ObjectDataInput} and converts to a neighbourhood
     * @param in the object data to be deserialized
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
    }

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     * @param o the {@link Neighbourhood} to which this one is to be checked for equality
     * @return true if the underlying objects of the Neighbourhood are both considered equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Neighbourhood)) return false;
        Neighbourhood that = (Neighbourhood) o;
        return name.equals(that.name);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     * @return a hashcode of the Neighbourhood
     */
    @Override
    public int hashCode() {
        return (name == null ? 0 : name.hashCode());
    }

    /**
     * Simply overrides the string printing for this class
     * @return the resulting print string
     */
    @Override
    public String toString() {
        return "Neighbourhood{" +
                "name='" + name + '\'' +
                '}';
    }
}
