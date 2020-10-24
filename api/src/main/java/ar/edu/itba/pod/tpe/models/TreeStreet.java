package ar.edu.itba.pod.tpe.models;


import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;
import java.util.Objects;

public class TreeStreet implements DataSerializable {

    private String neighbourhood;
    private String tree;

    public TreeStreet() {
    }

    public TreeStreet(String neighbourhood, String tree) {
        this.neighbourhood = neighbourhood;
        this.tree = tree;
    }

    public String getNeighbourhood() {
        return neighbourhood;
    }

    public String getTree() {
        return tree;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TreeStreet that = (TreeStreet) o;
        return Objects.equals(neighbourhood, that.neighbourhood) &&
                Objects.equals(tree, that.tree);
    }

    @Override
    public int hashCode() {
        return Objects.hash(neighbourhood, tree);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(neighbourhood);
        out.writeUTF(tree);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        neighbourhood = in.readUTF();
        tree = in.readUTF();
    }
}
