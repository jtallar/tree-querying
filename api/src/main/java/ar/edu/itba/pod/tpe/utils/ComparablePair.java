package ar.edu.itba.pod.tpe.utils;

/**
 * Comparable Pair. A pair which can be sorted.
 * @param <F> The first object, extends comparable.
 * @param <S> The second object, who also extends from comparable.
 */
public class ComparablePair<F extends Comparable<F>, S extends Comparable<S>>
        implements Comparable<ComparablePair<F, S>> {
    private F first;
    private S second;

    /**
     * Constructor for a ComparablePair.
     *
     * @param first the first object in the ComparablePair
     * @param second the second object in the pair
     */
    public ComparablePair(F first, S second) {
        this.first = first;
        this.second = second;
    }

    /** Getters */

    public F getFirst() {
        return first;
    }

    public S getSecond() {
        return second;
    }

    /** Setters */

    public void setFirst(F first) {
        this.first = first;
    }

    public void setSecond(S second) {
        this.second = second;
    }

    /**
     * Checks the two objects for equality by delegating to their respective
     * {@link Object#equals(Object)} methods.
     * @param o the {@link ComparablePair} to which this one is to be checked for equality
     * @return true if the underlying objects of the ComparablePair are both considered
     *         equal
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ComparablePair)) return false;
        ComparablePair<?, ?> p = (ComparablePair<?, ?>) o;
        return first.equals(p.first) &&
                second.equals(p.second);
    }

    /**
     * Compute a hash code using the hash codes of the underlying objects
     * @return a hashcode of the ComparablePair
     */
    @Override
    public int hashCode() {
        return (first == null ? 0 : first.hashCode()) ^ (second == null ? 0 : second.hashCode());
    }

    /**
     * Compares this object with the specified object for order.
     * @return a negative integer, zero, or a positive integer as this object is less
     * than, equal to, or greater than the specified object.
     */
    @Override
    public int compareTo(ComparablePair<F, S> o) {
        int c = first.compareTo(o.first);
        return (c != 0) ? c : second.compareTo(o.second);
    }

    /**
     * Compares this object with the specified object for order.
     * It inverts the order of the first field.
     * @return a negative integer, zero, or a positive integer as this object is greater
     * than, equal to, or less than the specified object.
     */
    public int compareToModified(ComparablePair<F, S> o) {
        int c = first.compareTo(o.first) * (-1);
        return (c != 0) ? c : second.compareTo(o.second);
    }

    /**
     * Simply overrides the string printing for this class
     * @return the resulting print string
     */
    @Override
    public String toString() {
        return "ComparablePair{" +
                "first=" + first +
                ", second=" + second +
                "}\n";
    }
}
