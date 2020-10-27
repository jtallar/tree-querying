package ar.edu.itba.pod.tpe.utils;

/**
 * Comparable Trio. A Trio which can be sorted.
 * @param <F> The first object, extends comparable.
 * @param <S> The second object, extends comparable.
 * @param <T> The third object, who also extends from comparable.
 */
public class ComparableTrio <F extends Comparable<F>, S extends Comparable<S>, T extends Comparable<T>>
        implements Comparable<ComparableTrio<F, S, T>> {
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

    /** Getters */

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
     * @param o the {@link ComparableTrio} to which this one is to be checked for equality
     * @return true if the underlying objects of the ComparableTrio are both considered
     *         equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparableTrio)) return false;
        ComparableTrio<?, ?, ?> that = (ComparableTrio<?, ?, ?>) o;
        return first.equals(that.getFirst()) &&
                second.equals(that.getSecond()) &&
                third.equals(that.getThird());
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     * @return a hashcode of the ComparablePair
     */
    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^
                (second == null ? 0 : second.hashCode()) ^
                (third == null ? 0 : third.hashCode());
    }

    /**
     * Compares this object with the specified object for order.
     * @return a negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(ComparableTrio<F, S, T> o) {
        int c1 = first.compareTo(o.getFirst());
        if (c1 != 0) return c1;

        int c2 = second.compareTo(o.getSecond());
        return (c2 != 0) ? c2 : third.compareTo(o.getThird());
    }

    /**
     * Compares this object with the specified object for order.
     * It inverts the order of the first field.
     * @return a negative integer, zero, or a positive integer as this object is greater
     * than, equal to, or less than the specified object.
     */
    public int compareToModified(ComparableTrio<F, S, T> o) {
        int c1 = first.compareTo(o.getFirst()) * (-1);
        if (c1 != 0) return c1;

        int c2 = second.compareTo(o.getSecond());
        return (c2 != 0) ? c2 : third.compareTo(o.getThird());
    }

    /**
     * Simply overrides the string printing for this class
     * @return the resulting print string
     */
    @Override
    public String toString() {
        return "ComparableTrio{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                "}\n";
    }
}
