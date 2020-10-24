package ar.edu.itba.pod.tpe.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Tree implements DataSerializable {
    private String street_name;
    private String common_name;
    private double diameter;

    // Empty constructor for Hazelcast
    public Tree() {

    }

    public Tree(String street_name, String common_name, double diameter) {
        this.street_name = street_name;
        this.common_name = common_name;
        this.diameter = diameter;
    }

    public String getStreet_name() {
        return street_name;
    }

    public String getCommon_name() {
        return common_name;
    }

    public double getDiameter() {
        return diameter;
    }

    @Override
    public String toString() {
        return street_name + "--" + common_name + "--" +  diameter;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(street_name);
        out.writeUTF(common_name);
        out.writeDouble(diameter);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        street_name = in.readUTF();
        common_name = in.readUTF();
        diameter = in.readDouble();
    }
}
