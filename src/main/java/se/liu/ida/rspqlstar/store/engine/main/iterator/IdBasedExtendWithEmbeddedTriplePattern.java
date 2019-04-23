package se.liu.ida.rspqlstar.store.engine.main.iterator;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.SolutionMapping;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Element;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Key;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.engine.main.quadpattern.Variable;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.util.Collections;
import java.util.Iterator;

/**
 * Iterator for the bind operator.
 *
 * If concrete, add var -> refT to solMap and return, else
 * iterate all quads in pattern, and bind.
 */
public class IdBasedExtendWithEmbeddedTriplePattern implements Iterator<SolutionMapping>, Closeable {
    final private ExecutionContext execCxt;
    final private QuadStarPattern pattern;
    final private DatasetGraphStar datasetGraph;
    final private Iterator<SolutionMapping> input;
    private SolutionMapping currentInputMapping = null;
    private QuadStarPattern currentQueryPattern = null;
    private Iterator<? extends IdBasedQuad> currentMatches = null;

    final private int var;
    static private ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public IdBasedExtendWithEmbeddedTriplePattern(int var, QuadStarPattern pattern, Iterator<SolutionMapping> input,
                                                  ExecutionContext execCxt) {
        this.var = var;
        this.pattern = pattern;
        this.input = input;
        this.execCxt = execCxt;
        this.datasetGraph = ((StreamingDatasetGraph) execCxt.getDataset()).getActiveDataset();
    }

    public boolean hasNext() {
        while (currentMatches == null || !currentMatches.hasNext()) {
            if (!input.hasNext() || !pattern.isMatchable()) {
                return false;
            }
            currentInputMapping = input.next();
            currentQueryPattern = substitute(pattern, currentInputMapping);
            //currentQueryPattern = reproduce(var, currentQueryPattern, currentInputMapping);

            if (currentQueryPattern.isConcrete()) {
                if (datasetGraph.contains(currentQueryPattern)) {
                    currentMatches = Collections.singleton(new IdBasedQuad(
                            currentQueryPattern.graph.asKey().id,
                            currentQueryPattern.subject.asKey().id,
                            currentQueryPattern.predicate.asKey().id,
                            currentQueryPattern.object.asKey().id)).iterator();
                } else {
                    currentMatches = Collections.<IdBasedQuad>emptyList().iterator();
                }
            } else {
                currentMatches = datasetGraph.find(currentQueryPattern);
            }
        }
        return true;
    }

    public SolutionMapping next() {
        if (!hasNext()) {
            throw new IllegalStateException();
        }

        // Create the next solution mapping
        // i) copy the mapping currently consumed from the input iterator, and
        // ii) bind the variables in the copy corresponding to the currently matching triple
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
            // Note: currentMatch is concrete and there is no key we have to resort to using a wrapper
            final IdBasedTriple tp = new IdBasedTriple(currentMatch.subject, currentMatch.predicate, currentMatch.object);
            result.set(var, new TripleWrapperKey(tp));
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
     * Replaces each query variable in the given quad pattern that based on the solution mapping.
     *
     * @param quadStarPattern
     * @param solMap
     * @return
     */
     public QuadStarPattern substitute(QuadStarPattern quadStarPattern, SolutionMapping solMap) {
        final Element g = tryToSubstitute(quadStarPattern.graph, solMap);
        final Element s = tryToSubstitute(quadStarPattern.subject, solMap);
        final Element p = tryToSubstitute(quadStarPattern.predicate, solMap);
        final Element o = tryToSubstitute(quadStarPattern.object, solMap);
        return new QuadStarPattern(g, s, p, o);
    }

    /**
     * Reproduce bindings for a triple pattern if there exists a binding for var. If a conflict is detected null is
     * returned.
     *
     * @param var
     * @param tp
     * @param solMap
     * @returnsy
     */
    static public QuadStarPattern reproduce(int var, QuadStarPattern tp, SolutionMapping solMap) {
        final Key tripleKey = solMap.get(var);
        if (tripleKey == null) {
            // nothing to reproduce
            return tp;
        }

        final long s, p, o;
        if (IdFactory.isReferenceId(tripleKey.id)) {
            final IdBasedTriple idBasedTriple = refT.getIdBasedTriple(tripleKey.id);
            s = idBasedTriple.subject;
            p = idBasedTriple.predicate;
            o = idBasedTriple.object;
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
            solMap.set(tp.subject.asVariable().varId, sKey);
        }
        if (tp.predicate.isVariable()) {
            solMap.set(tp.predicate.asVariable().varId, pKey);
        }
        if (tp.object.isVariable()) {
            solMap.set(tp.object.asVariable().varId, oKey);
        }

        // TODO
        return new QuadStarPattern(null, sKey, pKey, oKey);
    }

    private static Key getTripleKey(long s, long p, long o) {
        // Try to get reference node
        final Long id = refT.getId(new IdBasedTriple(s, p, o));
        return id != null ? new Key(id) : null;
    }

    /**
     * Try to map the element to a value if it is a variable, otherwise return the element.
     * @param el
     * @param solMap
     * @return
     */
    public Element tryToSubstitute(final Element el, final SolutionMapping solMap){
        if(el.isVariable()) {
            final Variable var = el.asVariable();
            if (solMap.contains(var.varId)) {
                return solMap.get(var.varId);
            }
        }
        return el;
    }
}