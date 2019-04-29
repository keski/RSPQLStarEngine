package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;
import se.liu.ida.rspqlstar.store.engine.main.pattern.Key;

import java.util.Iterator;

public class IdBasedExtendIterator implements Iterator<SolutionMapping> {

    private final int var;
    private final Key key;
    private final Iterator<SolutionMapping> input;

    public IdBasedExtendIterator(int var, Key key, Iterator<SolutionMapping> input, ExecutionContext execCxt) {
        this.var = var;
        this.key = key;
        this.input = input;
    }

    @Override
    public boolean hasNext() {
        return input.hasNext();
    }

    @Override
    public SolutionMapping next() {
        final SolutionMapping solMap = input.next();
        solMap.set(var, key);
        return solMap;
    }
}
