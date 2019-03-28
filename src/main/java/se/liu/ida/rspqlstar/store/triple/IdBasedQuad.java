package se.liu.ida.rspqlstar.store.triple;

import se.liu.ida.rspqlstar.store.triplestore.Field;

import java.io.Serializable;
import java.util.Objects;

/**
 * ID encoded quad.
 */

public class IdBasedQuad implements Comparable<IdBasedQuad>, Serializable {
    final public long graph;
    final public long subject;
    final public long predicate;
    final public long object;

    public IdBasedQuad(long graph, long subject, long predicate, long object) {
        this.graph = graph;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public String toString() {
        return String.format("%s { %s %s %s }", graph, subject, predicate, object);
    }

    public long get(Field field) {
        if (field == Field.G) {
            return graph;
        } else if (field == Field.S) {
            return subject;
        } else if (field == Field.P) {
            return predicate;
        } else if (field == Field.O) {
            return object;
        } else {
            throw new IllegalStateException("The field " + field + " is not defined for QuadStar objects");
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(graph, subject, predicate, object);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IdBasedQuad) {
            IdBasedQuad quad = ((IdBasedQuad) other);
            return graph == quad.graph &&
                    subject == quad.subject &&
                    predicate == quad.predicate &&
                    object == quad.object;
        } else {
            return super.equals(other);
        }
    }

    @Override
    public int compareTo(IdBasedQuad other) {
        // graph
        if (graph > other.graph) {
            return 1;
        } else if (graph < other.graph) {
            return -1;
        }
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
