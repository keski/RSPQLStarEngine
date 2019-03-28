package se.liu.ida.rspqlstar.store.triplestore;

import java.util.Iterator;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;

/**
 * An index is the physical storage of the quads.
 */
public interface Index {
    void add(final IdBasedQuad q);

    long size();

    boolean contains(QuadStarPattern quadStarPattern);

    boolean contains(IdBasedQuad idBasedQuad);

    Iterator<IdBasedQuad> iterator(QuadStarPattern triple);

    Iterator<IdBasedQuad> iterateAll();

    void print(int limit);
}
