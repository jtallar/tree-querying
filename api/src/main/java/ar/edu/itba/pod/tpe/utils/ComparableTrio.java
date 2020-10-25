package ar.edu.itba.pod.tpe.utils;

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

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     *
     * @param o the {@link ComparablePair} to which this one is to be checked for equality
     * @return true if the underlying objects of the ComparablePair are both considered
     *         equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparableTrio)) return false;
        ComparableTrio<?, ?, ?> p = (ComparableTrio<?, ?, ?>) o;
        return first.equals(p.first) && second.equals(p.second) && third.equals(p.third);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     *
     * @return a hashcode of the ComparablePair
     */
    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode()) ^ (third == null ? 0 : third.hashCode());
    }

    @Override
    public int compareTo(ComparableTrio<F, S, T> o) {
        int c1 = first.compareTo(o.first);
        if (c1 != 0) {
            int c2 = second.compareTo(o.second);
            return (c2 != 0) ? c2 : third.compareTo(o.third);
        }
        return c1;
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
