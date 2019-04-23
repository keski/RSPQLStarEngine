package se.liu.ida.rspqlstar.store.index;

import java.io.Serializable;
import java.util.Objects;

/**
 * ID encoded triple.
 */

public class IdBasedTriple implements Comparable<IdBasedTriple>, Serializable {
    final public long subject;
    final public long predicate;
    final public long object;

    public IdBasedTriple( long subject, long predicate, long object) {
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public String toString() {
        return String.format("%s %s %s", subject, predicate, object);
    }

    public long get(Field field) {
        if (field == Field.S) {
            return subject;
        } else if (field == Field.P) {
            return predicate;
        } else if (field == Field.O) {
            return object;
        } else {
            throw new IllegalStateException("The field " + field + " is not defined");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(subject, predicate, object);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IdBasedTriple) {
            IdBasedTriple quad = ((IdBasedTriple) other);
            return subject == quad.subject &&
                    predicate == quad.predicate &&
                    object == quad.object;
        } else {
            return super.equals(other);
        }
    }

    @Override
    public int compareTo(IdBasedTriple other) {
        // subject
        if (subject > other.subject) {
            return 1;
        } else if (subject < other.subject) {
            return -1;
        }
        // predicate
        if (predicate > other.predicate) {
            return 1;
        } else if (predicate < other.predicate) {
            return -1;
        }
        // object
        if (object > other.object) {
            return 1;
        } else if (object < other.object) {
            return -1;
        }
        return 0;
    }
}
