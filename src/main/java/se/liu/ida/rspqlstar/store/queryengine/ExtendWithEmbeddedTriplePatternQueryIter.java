package se.liu.ida.rspqlstar.store.queryengine;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triple.IdFactory;
import se.liu.ida.rspqlstar.store.triplepattern.Element;
import se.liu.ida.rspqlstar.store.triplepattern.Key;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.triplestore.QuadStore;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Iterator for the bind operator
 */
public class ExtendWithEmbeddedTriplePatternQueryIter implements Iterator<SolutionMapping>, Closeable {
    final private ExecutionContext execCxt;

    /**
     * The triple pattern matched
     */
    final private QuadStarPattern tp;

    /**
     * The input iterator consumed
     */
    final private Iterator<SolutionMapping> input;

    /**
     * The solution mapping from the input iterator
     */
    private SolutionMapping currentInputMapping = null;

    /**
     * The current query pattern is the triple pattern of this iterator
     * (see {@link #tp}) substituted with the bindings provided by the
     * current solution mapping.
     */
    private QuadStarPattern currentQueryPattern = null;

    /**
     * an iterator over all triples that match the current query pattern
     * (see {@link #currentQueryPattern}) in the queried dataset
     */
    private Iterator<? extends IdBasedQuad> currentMatches = null;

    final private int var;
    static private ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public ExtendWithEmbeddedTriplePatternQueryIter(
            int var,
            QuadStarPattern tp,
            Iterator<SolutionMapping> input,
            ExecutionContext execCxt) {
        this.var = var;
        this.tp = tp;
        this.input = input;
        this.execCxt = execCxt;
    }

    public boolean hasNext() {
        System.out.println("ExtendWithEmbedded");
        while (currentMatches == null || !currentMatches.hasNext()) {
            if (!input.hasNext() || !tp.isMatchable()) {
                return false;
            }
            /*
            final Graph graph = (Graph) execCxt.getActiveGraph();
            currentInputMapping = input.next();
            currentQueryPattern = substitute(tp, currentInputMapping);
            // BIND case 2 in the google docs: "(s,p,o) AS ?t and ?t comes in"
            currentQueryPattern = reproduce(var, currentQueryPattern, currentInputMapping);

            if (currentQueryPattern == null) {
                currentMatches = null;
            } else if (currentQueryPattern.isConcrete()) {
                boolean match = graph.graphBaseContains(currentQueryPattern);
                if (match) {
                    currentMatches = Collections.singleton(new IdBasedTriple(
                            currentQueryPattern.subject.asKey().id,
                            currentQueryPattern.predicate.asKey().id,
                            currentQueryPattern.object.asKey().id)).iterator();
                } else {
                    currentMatches = Collections.<IdBasedTriple>emptyList().iterator();
                }
            } else {
                // Returns all triples, even if not nested
                currentMatches = graph.graphBaseFind(currentQueryPattern);
            }*/
        }
        return true;
    }

    public SolutionMapping next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Create the next solution mapping by i) copying the mapping currently
        // consumed from the input iterator and ii) by binding the variables in
        // the copy corresponding to the currently matching triple (currentMatch).
        final IdBasedQuad currentMatch = currentMatches.next();
        final SolutionMapping result = new SolutionMapping(currentInputMapping);

        // Set bindings for each variable
        if (currentQueryPattern.subject.isVariable()) {
            result.set(currentQueryPattern.subject.asVariable().varId, new Key(currentMatch.subject));
        }

        if (currentQueryPattern.predicate.isVariable()) {
            result.set(currentQueryPattern.predicate.asVariable().varId, new Key(currentMatch.predicate));
        }

        if (currentQueryPattern.object.isVariable()) {
            result.set(currentQueryPattern.object.asVariable().varId, new Key(currentMatch.object));
        }

        // Set solution mappings for the bind variable
        final Key key = getTripleKey(currentMatch.subject, currentMatch.predicate, currentMatch.object);
        if (key != null) {
            result.set(var, key);
        } else {
            // Note: currentMatch is concrete and if there is no key we have to resort to using a DummyKey
            // TODO
            final IdBasedQuad tp = new IdBasedQuad(QuadStore.DEFAULT_GRAPH_ID, currentMatch.subject, currentMatch.predicate, currentMatch.object);
            result.set(var, new DummyKey(tp));
        }
        return result;
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (input instanceof Closeable) {
            ((Closeable) input).close();
        }
    }

    /**
     * Replaces each query variable in the given triple pattern that is bound to
     * a value in the given solution mapping by this value.
     *
     * @param triplePattern
     * @param solutionMapping
     * @return
     */
    static public QuadStarPattern substitute(QuadStarPattern triplePattern, SolutionMapping solutionMapping) {
        final Element s, p, o;
        // Subject
        if (triplePattern.subject.isVariable()) {
            final int var = triplePattern.subject.asVariable().varId;
            if(solutionMapping.contains(var)){
                s = solutionMapping.get(var);
            } else {
                s = triplePattern.subject;
            }
        } else {
            s = triplePattern.subject;
        }
        // Predicate
        if (triplePattern.predicate.isVariable()) {
            final int var = triplePattern.predicate.asVariable().varId;
            if(solutionMapping.contains(var)){
                p = solutionMapping.get(var);
            } else {
                p = triplePattern.predicate;
            }
        } else {
            p = triplePattern.predicate;
        }
        // Object
        if (triplePattern.object.isVariable()) {
            final int var = triplePattern.object.asVariable().varId;
            if(solutionMapping.contains(var)){
                o = solutionMapping.get(var);
            } else {
                o = triplePattern.object;
            }
        } else {
            o = triplePattern.object;
        }

        // TODO
        return new QuadStarPattern(null, s, p, o);
    }

    /**
     * Reproduce bindings for a triple pattern if there exists a binding for var. If a conflict is detected null is
     * returned.
     *
     * @param var
     * @param tp
     * @param solutionMapping
     * @returnsy
     */
    static public QuadStarPattern reproduce(int var, QuadStarPattern tp, SolutionMapping solutionMapping) {
        final Key tripleKey = solutionMapping.get(var);
        if (tripleKey == null) {
            // nothing to reproduce
            return tp;
        }

        final long s, p, o;
        if (IdFactory.isReferenceId(tripleKey.id)) {
            final IdBasedQuad idBasedTripleQuad = refT.getIdBasedQuad(tripleKey.id);
            s = idBasedTripleQuad.subject;
            p = idBasedTripleQuad.predicate;
            o = idBasedTripleQuad.object;
        } else {
            return null;
        }

        // Check that each value matches existing bindings
        if (tp.subject.isConcrete() && tp.subject.asKey().id != s) {
            return null;
        } else if (tp.predicate.isConcrete() && tp.predicate.asKey().id != p) {
            return null;
        } else if (tp.object.isConcrete() && tp.object.asKey().id != o) {
            return null;
        }

        // Wrap ID as keys
        final Key sKey = new Key(s);
        final Key pKey = new Key(p);
        final Key oKey = new Key(o);

        // Set mappings
        if (tp.subject.isVariable()) {
            solutionMapping.set(tp.subject.asVariable().varId, sKey);
        }
        if (tp.predicate.isVariable()) {
            solutionMapping.set(tp.predicate.asVariable().varId, pKey);
        }
        if (tp.object.isVariable()) {
            solutionMapping.set(tp.object.asVariable().varId, oKey);
        }

        // TODO
        return new QuadStarPattern(null, sKey, pKey, oKey);
    }

    private static Key getTripleKey(long s, long p, long o) {
        // Try to get reference node
        final Long id = refT.getId(new IdBasedQuad(QuadStore.DEFAULT_GRAPH_ID, s, p, o));
        if(id != null){
            return new Key(id);
        }
        return null;
    }
}