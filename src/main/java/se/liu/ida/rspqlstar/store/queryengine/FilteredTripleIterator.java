package se.liu.ida.rspqlstar.store.queryengine;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.Element;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.Field;

import java.util.Iterator;

/**
 * The FilteredTripleIterator provides a simple way of filtering out triples that contain incompatible bindings for
 * a given TripleStarPattern. For example, given the triple pattern "?s ?p ?s" both the subject and object must be equal.
 * This is not handled by the indexes, which only look up matching triples based on concrete values.
 */

public class FilteredTripleIterator implements Iterator {
    private enum FilterType {
        SPO,
        SP,
        SO,
        PO,
        DEFAULT
    }

    final private Iterator<IdBasedQuad> iterator;
    final private FilterType filterType;

    private boolean hasNext = true;
    private IdBasedQuad next;

    public FilteredTripleIterator(final Iterator<IdBasedQuad> iterator, QuadStarPattern tp) {
        this.iterator = iterator;

        final Element s = tp.getField(Field.S);
        final Element p = tp.getField(Field.P);
        final Element o = tp.getField(Field.O);

        if (s.isVariable() && p.isVariable() && o.isVariable() && s.equals(p) && p.equals(o)) {
            filterType = FilterType.SPO;
        } else if (s.isVariable() && p.isVariable() && s.equals(p)) {
            filterType = FilterType.SP;
        } else if (s.isVariable() && o.isVariable() && s.equals(o)) {
            filterType = FilterType.SO;
        } else if (p.isVariable() && o.isVariable() && p.equals(o)) {
            filterType = FilterType.PO;
        } else {
            filterType = FilterType.DEFAULT;
        }
        this.findNext();
    }

    @Override
    public boolean hasNext() {
        return next != null;
    }

    @Override
    public IdBasedQuad next() {
        final IdBasedQuad returnValue = next;
        findNext();
        return returnValue;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    private void findNext() {
        while (iterator.hasNext()) {
            next = iterator.next();
            switch (filterType) {
                case SPO:
                    if (next.subject == next.predicate && next.predicate == next.object) {
                        return;
                    }
                    break;
                case SP:
                    if (next.subject == next.predicate) {
                        return;
                    }
                    break;
                case SO:
                    if (next.subject == next.object) {
                        return;
                    }
                    break;
                case PO:
                    if (next.predicate == next.object) {
                        return;
                    }
                    break;
                default:
                    return;
            }
        }
        this.next = null;
        this.hasNext = false;
    }
}
