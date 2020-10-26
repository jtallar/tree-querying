package ar.edu.itba.pod.tpe.models;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class Neighbourhood implements DataSerializable {

    private String name;
    private int population;

    // Empty constructor for Hazelcast
    public Neighbourhood() {
    }

    public Neighbourhood(String name) {
        this.name = name;
        this.population = 0;
    }

    public Neighbourhood(String name, Integer population) {
        this.name = name;
        this.population = population;
    }

    public String getName() {
        return name;
    }

    public void setPopulation(int population) {
        this.population = population;
    }

    public int getPopulation() { return this.population; }

    @Override
    public String toString() {
        return   name + "--" + population;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Neighbourhood)) return false;
        Neighbourhood that = (Neighbourhood) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(population);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        name = in.readUTF();
        population = in.readInt();
    }
}
