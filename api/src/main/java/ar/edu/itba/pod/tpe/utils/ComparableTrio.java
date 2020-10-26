package ar.edu.itba.pod.tpe.utils;

import java.util.Objects;

public class ComparableTrio <F extends Comparable<F>, S extends Comparable<S>, T extends Comparable<T>> implements Comparable<ComparableTrio<F, S, T>> {
    private final F first;
    private final S second;
    private final T third;

    /**
     * Constructor for a ComparablePair.
     *
     * @param first the first object in the ComparablePair
     * @param second the second object in the pair
     */
    public ComparableTrio(F first, S second, T third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    public T getThird() {
        return third;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparableTrio)) return false;
        ComparableTrio<?, ?, ?> that = (ComparableTrio<?, ?, ?>) o;
        return first.equals(that.getFirst()) &&
                second.equals(that.getSecond()) &&
                third.equals(that.getThird());
    }

    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^
                (second == null ? 0 : second.hashCode()) ^
                (third == null ? 0 : third.hashCode());
    }

    @Override
    public int compareTo(ComparableTrio<F, S, T> o) {
        int c1 = first.compareTo(o.getFirst()) * (-1);
        if (c1 != 0) return c1;

        int c2 = second.compareTo(o.getSecond());
        return (c2 != 0) ? c2 : third.compareTo(o.getThird());
    }

    @Override
    public String toString() {
        return "ComparableTrio{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                "}\n";
    }
}
