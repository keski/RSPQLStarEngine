package se.liu.ida.rspqlstar.store.queryengine;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.Key;

public class DummyKey extends Key {
    final public IdBasedQuad idBasedQuad;

    public DummyKey(IdBasedQuad idBasedTriple) {
        super(0L);
        this.idBasedQuad = idBasedTriple;
    }
}
