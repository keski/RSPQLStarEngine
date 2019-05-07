package se.liu.ida.rspqlstar.store.index;

import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;

import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * Hashindex. Fast but simple. Used only for timestamped graphs.
 */

public class HashIndex extends AbstractIndex implements Index {
    final HashSet<IdBasedQuad> index = new HashSet<>();

    public HashIndex(Field field1, Field field2, Field field3, Field field4){
        super(field1, field2, field3, field4);
    }

    public int compare(long l1, long l2){
        if(l1 < l2){
            return -1;
        } else if(l1 > l2){
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public void add(IdBasedQuad idBasedQuad) {
        index.add(idBasedQuad);
    }

    @Override
    public long size() {
        return index.size();
    }

    @Override
    public boolean contains(QuadStarPattern p) {
        throw new IllegalStateException();
    }

    @Override
    public boolean contains(IdBasedQuad q) {
        return index.contains(q);
    }

    @Override
    public Iterator<IdBasedQuad> iterator(QuadStarPattern p) {
        throw new IllegalStateException();
    }

    @Override
    public Iterator<IdBasedQuad> iterateAll() {
        return index.iterator();
    }
}
