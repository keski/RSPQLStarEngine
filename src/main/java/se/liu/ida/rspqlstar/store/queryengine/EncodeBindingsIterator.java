package se.liu.ida.rspqlstar.store.queryengine;


import java.util.Iterator;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;

import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.triplepattern.Key;


/**
 * Iterator for encoding solution mappings (bindings)
 */
public class EncodeBindingsIterator implements Iterator<SolutionMapping>, Closeable {
    /** The input iterator consumed */
    final private QueryIterator input;
    final private NodeDictionary nd = NodeDictionaryFactory.get();
    final private VarDictionary varDict = VarDictionary.get();

    public EncodeBindingsIterator(QueryIterator input, ExecutionContext execCxt) {
        this.input = input;
    }

    public boolean hasNext() {
        return input.hasNext();
    }

    public SolutionMapping next() {
        final Binding curInput = input.next();

        if (!curInput.isEmpty()) {
            throw new IllegalStateException("curInput is not empty!");
        }

        final SolutionMapping curOutput = new SolutionMapping(varDict.size());
        final Iterator<Var> itVar = curInput.vars();
        while (itVar.hasNext()) {
            final Var var = itVar.next();
            curOutput.set(varDict.getId(var), new Key(nd.getId(curInput.get(var))));
        }
        return curOutput;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        input.close();
    }
}
