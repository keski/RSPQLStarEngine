package se.liu.ida.rspqlstar.store.engine.main.iterator;

import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Element;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.index.Field;

import java.util.Iterator;

/**
 * The FilteredTripleIterator provides a simple way of filtering out triples that contain incompatible bindings for
 * a given TripleStarPattern. For example, given the triple pattern "?s ?p ?s" both the subject and object must be equal.
 * This is not handled by the indexes, which only look up matching triples based on concrete values.
 */

public class FilteredQuadIterator implements Iterator {
    private enum FilterType {
        GS, GP, GO, SP, SO, PO,
        GSP, GSO, GPO, SPO,
        GSPO,
        DEFAULT
    }

    final private Iterator<IdBasedQuad> iterator;
    final private FilterType filterType;

    private boolean hasNext = true;
    private IdBasedQuad next;

    public FilteredQuadIterator(final Iterator<IdBasedQuad> iterator, QuadStarPattern tp) {
        this.iterator = iterator;

        final Element g = tp.getField(Field.G);
        final Element s = tp.getField(Field.S);
        final Element p = tp.getField(Field.P);
        final Element o = tp.getField(Field.O);

        int type = 0;
        if(g.isVariable()) type += 1;
        if(s.isVariable()) type += 2;
        if(p.isVariable()) type += 4;
        if(o.isVariable()) type += 8;

        switch(type) {
            case 3: if(g == s) { filterType = FilterType.GS; break; }
            case 5: if(g == p) { filterType = FilterType.GP; break; }
            case 6: if(s == p) { filterType = FilterType.SP; break; }
            case 7: if(g == s && s == p) { filterType = FilterType.GSP; break; }
            case 9: if(g == o) { filterType = FilterType.GO; break; }
            case 10: if(s == o) { filterType = FilterType.SO; break; }
            case 11: if(g == s && s == o) { filterType = FilterType.GSO; break; }
            case 12: if(p == o) { filterType = FilterType.PO; break; }
            case 13: if(g == p && p == o) { filterType = FilterType.GPO; break; }
            case 14: if(s == p && p == o) { filterType = FilterType.SPO; break; }
            case 15: if(g == s && s == p && p == o) { filterType = FilterType.GSPO; break; }
            default: filterType = FilterType.DEFAULT;
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
                case GSPO:
                    if(next.graph == next.subject && next.subject == next.predicate && next.predicate == next.object)
                        return;
                    break;
                case GSP:
                    if(next.graph == next.subject && next.subject == next.predicate)
                        return;
                    break;
                case GSO:
                    if(next.graph == next.subject && next.subject == next.object)
                        return;
                    break;
                case GPO:
                    if (next.graph == next.predicate && next.predicate == next.object)
                        return;
                    break;
                case SPO:
                    if (next.subject == next.predicate && next.predicate == next.object)
                        return;
                    break;
                case GS:
                    if (next.graph == next.subject)
                        return;
                    break;
                case GP:
                    if (next.graph == next.predicate)
                        return;
                    break;
                case GO:
                    if (next.graph == next.object)
                        return;
                    break;
                case SP:
                    if (next.subject == next.predicate)
                        return;
                    break;
                case SO:
                    if (next.subject == next.object)
                        return;
                    break;
                case PO:
                    if (next.predicate == next.object)
                        return;
                    break;
                default:
                    return;
            }
        }
        this.next = null;
        this.hasNext = false;
    }
}
