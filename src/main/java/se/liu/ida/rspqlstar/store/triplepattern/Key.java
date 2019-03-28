package se.liu.ida.rspqlstar.store.triplepattern;

import java.io.Serializable;

import se.liu.ida.rspqlstar.store.triple.IdFactory;
import se.liu.ida.rspqlstar.store.utils.Configuration;

/**
 * A key encompasses three types of triple elements:
 * 1) a URI or Literal
 * 2) an embedded triple
 * 3) a reference triple
 * <p>
 * The supported encodings are as follows:
 * 10- the key is an embedded triple.
 * 11- the key is a reference triple.
 * 0- the key is a URI or Literal (Note: All positive numbers are encoded URIs or Literals).
 */
public class Key extends Element implements Comparable<Key>, Serializable {
    final public long id;

    public Key(long id) {
        this.id = id;
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof Key) {
            return id == ((Key) o).id;
        } else {
            return super.equals(o);
        }
    }

    @Override
    public int hashCode() {
        return (int) id;
    }

    @Override
    public boolean isKey() {
        return true;
    }

    @Override
    public int compareTo(Key other) {
        return Long.compare(id, other.id);
    }

    /**
     * Produces a 64 bit string for the key.
     *
     * @return
     */
    public String toString() {
        return String.format("%64s", Long.toBinaryString(id)).replace(" ", "0");
    }

    /**
     * Produces a pretty printed human readable version of key.
     *
     * @return
     */
    public String getPrettyString() {
        if (IdFactory.isReferenceId(id)) {
            return "1-" + toString().substring(1);
        }
        return toString();
    }

    /**
     * Produces a bit string for a long.
     *
     * @param l
     * @return
     */
    public static String makeBinaryString(long l, int bitSize) {
        return String.format("%" + bitSize + "s", Long.toBinaryString(l)).replace(" ", "0");
    }
}
