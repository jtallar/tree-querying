package ar.edu.itba.pod.tpe.models;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class TreeStreet implements DataSerializable {

    private String neighbourhood;
    private String street;

    public TreeStreet() {
    }

    public TreeStreet(String neighbourhood, String street) {
        this.neighbourhood = neighbourhood;
        this.street = street;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public String getStreet() {
        return street;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeStreet that = (TreeStreet) o;
        return Objects.equals(neighbourhood, that.neighbourhood) &&
                Objects.equals(street, that.street);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbourhood, street);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(neighbourhood);
        out.writeUTF(street);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        neighbourhood = in.readUTF();
        street = in.readUTF();
    }
}
