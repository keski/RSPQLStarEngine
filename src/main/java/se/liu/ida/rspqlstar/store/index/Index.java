package se.liu.ida.rspqlstar.store.index;

import java.util.Iterator;

import se.liu.ida.rspqlstar.store.engine.main.quadpattern.QuadStarPattern;

/**
 * An index is the physical storage of the quads.
 */
public interface Index {
    enum Fields {
        GSPO,
        GPOS,
        GOSP,
        SPOG,
        POSG,
        OSPG
    }

    void add(final IdBasedQuad q);

    long size();

    boolean contains(QuadStarPattern quadStarPattern);

    boolean contains(IdBasedQuad idBasedQuad);

    Iterator<IdBasedQuad> iterator(QuadStarPattern triple);

    Iterator<IdBasedQuad> iterateAll();

    void print(int limit);
}
