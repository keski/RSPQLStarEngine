package se.liu.ida.rspqlstar.store.engine.main.iterator;

import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

public class TripleWrapperKey extends Key {
    final public IdBasedTriple idBasedTriple;

    public TripleWrapperKey(IdBasedTriple idBasedTriple) {
        super(0L); // any value
        this.idBasedTriple = idBasedTriple;
    }

    public String toString(){
        return String.format("TripleWrapperKey(%s)", idBasedTriple);
    }
}
