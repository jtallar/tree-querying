package ar.edu.itba.pod.tpe.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

/**
 * Serializable Data class modeling the trees information.
 */
public class Tree implements DataSerializable {

    private String streetName;
    private String commonName;
    private double diameter;

    /** Constructors */

    public Tree() {
        /** Empty constructor for Hazelcast */
    }

    public Tree(String streetName, String commonName, double diameter) {
        this.streetName = streetName;
        this.commonName = commonName;
        this.diameter = diameter;
    }

    /** Getters */

    public String getStreetName() {
        return streetName;
    }

    public String getCommonName() {
        return commonName;
    }

    public double getDiameter() {
        return diameter;
    }

    /**
     * Writes the data from a tree into a {@link ObjectDataOutput}
     * @param out the object data to be saved serialized into
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(streetName);
        out.writeUTF(commonName);
        out.writeDouble(diameter);
    }

    /**
     * Reads the data from a {@link ObjectDataInput} and converts to a tree
     * @param in the object data to be deserialized
     * @throws IOException when there is an error parsing the fields
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        streetName = in.readUTF();
        commonName = in.readUTF();
        diameter = in.readDouble();
    }

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     * @param o the {@link Tree} to which this one is to be checked for equality
     * @return true if the underlying objects of the Tree are both considered equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Tree)) return false;
        Tree tree = (Tree) o;
        return Double.compare(tree.diameter, diameter) == 0 &&
                streetName.equals(tree.streetName) &&
                commonName.equals(tree.commonName);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     * @return a hashcode of the Tree
     */
    @Override
    public int hashCode() {
        return (streetName == null ? 0 : streetName.hashCode()) ^
                (commonName == null ? 0: commonName.hashCode()) ^
                Double.hashCode(diameter);
    }

    /**
     * Simply overrides the string printing for this class
     * @return the resulting print string
     */
    @Override
    public String toString() {
        return "Tree{" +
                "streetName='" + streetName + '\'' +
                ", commonName='" + commonName + '\'' +
                ", diameter=" + diameter +
                '}';
    }
}
