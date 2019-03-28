package se.liu.ida.rspqlstar.store.triplepattern;

import org.apache.jena.reasoner.IllegalParameterException;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplestore.Field;

/**
 * A triple star pattern . The subject, predicate, or object can be a Key, a Variable, or another TripleStarPattern.
 * Note that it is vulnerable to circular references.
 */
public class QuadStarPattern extends Element {

    final public Element graph;
    final public Element subject;
    final public Element predicate;
    final public Element object;

    public QuadStarPattern(Element graph, Element subject, Element predicate, Element object) {
        this.graph = graph;
        this.subject = subject;
        this.predicate = predicate;
        this.object = object;
    }

    public boolean isMatchable() {
        return subject != null && predicate != null && object != null;
    }

    public boolean isFieldConcrete(Field field) {
        if (field == Field.G) {
            return graph != null && graph.isConcrete();
        } else if (field == Field.S) {
            return subject != null && subject.isConcrete();
        } else if (field == Field.P) {
            return predicate != null && predicate.isConcrete();
        } else if (field == Field.O) {
            return object != null && object.isConcrete();
        }
        throw new IllegalParameterException("Unknown Field " + field);
    }

    public String toString() {
        return graph + "(" + subject + ", " + predicate + ", " + object + ")";
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof IdBasedQuad) {
            return (this.hashCode() == other.hashCode());
        } else {
            return super.equals(other);
        }
    }

    public Element getField(Field field) {
        if (field == Field.G) {
            return graph;
        } else if (field == Field.S) {
            return subject;
        } else if (field == Field.P) {
            return predicate;
        } else if (field == Field.O) {
            return object;
        }
        throw new IllegalParameterException("Unknown Field " + field);
    }

    @Override
    public boolean isConcrete() {
        return graph.isConcrete() && subject.isConcrete() && predicate.isConcrete() && object.isConcrete();
    }
}
