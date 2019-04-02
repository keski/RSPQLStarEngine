package se.liu.ida.rspqlstar.store.queryengine;

import org.apache.jena.atlas.lib.Closeable;
import org.apache.jena.sparql.engine.ExecutionContext;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triplepattern.Element;
import se.liu.ida.rspqlstar.store.triplepattern.Key;
import se.liu.ida.rspqlstar.store.triplepattern.QuadStarPattern;

import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * Iterator for quad pattern
 */
public class QuadPatternQueryIter implements Iterator<SolutionMapping> {//}, Closeable {
    final private ExecutionContext execCxt;

    final private Iterator<SolutionMapping> input;

    // The pattern matched by this iterator
    final private QuadStarPattern pattern;

    // Incoming solution mapping
    private SolutionMapping solMap = null;

    // Iterator pattern with vars substituted based on incomingSolMap
    private QuadStarPattern currentQueryPattern = null;

    // Iterator over quads matching the iterator pattern
    private Iterator<? extends IdBasedQuad> currentMatches = null;

    public QuadPatternQueryIter(QuadStarPattern pattern, Iterator<SolutionMapping> input, ExecutionContext execCxt) {
        this.pattern = pattern;
        this.input = input;
        this.execCxt = execCxt;
    }

    public boolean hasNext() {
        System.out.println("QuadPatternIter");
        while (currentMatches == null || !currentMatches.hasNext()) {
            if (!input.hasNext() || !pattern.isMatchable()) {
                return false;
            }

            final DatasetGraphStar dsg = (DatasetGraphStar) execCxt.getDataset();
            solMap = input.next();
            currentQueryPattern = tryToSubstitute(pattern, solMap);

            if (currentQueryPattern == null) {
                return false;
            }

            // If tp contains no variables, contains is invoked instead.
            if (currentQueryPattern.isConcrete()) {
                boolean match = dsg.contains(currentQueryPattern);
                if (match) {
                    currentMatches = Collections.singleton(new IdBasedQuad(
                            currentQueryPattern.graph.asKey().id,
                            currentQueryPattern.subject.asKey().id,
                            currentQueryPattern.predicate.asKey().id,
                            currentQueryPattern.object.asKey().id)).iterator();
                } else {
                    currentMatches = Collections.<IdBasedQuad>emptyList().iterator();
                }
            } else {
                currentMatches = dsg.find(currentQueryPattern);
            }
        }
        return true;
    }

    public SolutionMapping next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        // Create the next solution mapping:
        // i) copy the mapping currently consumed from the input iterator, and
        // ii) binding the variables in the copy to the currently matching triple (currentMatch).
        final IdBasedQuad currentMatch = currentMatches.next();
        final SolutionMapping result = new SolutionMapping(solMap);

        //Add solution mappings for variables
        if (currentQueryPattern.graph.isVariable()) {
            result.set(currentQueryPattern.graph.asVariable().varId, new Key(currentMatch.graph));
        }

        if (currentQueryPattern.subject.isVariable()) {
            result.set(currentQueryPattern.subject.asVariable().varId, new Key(currentMatch.subject));
        }

        if (currentQueryPattern.predicate.isVariable()) {
            result.set(currentQueryPattern.predicate.asVariable().varId, new Key(currentMatch.predicate));
        }

        if (currentQueryPattern.object.isVariable()) {
            result.set(currentQueryPattern.object.asVariable().varId, new Key(currentMatch.object));
        }

        return result;
    }

    // implementation of the Closable interface
    public void close() {
        if (input instanceof Closeable) {
            ((Closeable) input).close();
        }
    }

    /**
     * Replaces each query variable in the given triple pattern that is bound to
     * a value in the given solution mapping by this value.
     */
    static public QuadStarPattern tryToSubstitute(QuadStarPattern pattern, SolutionMapping solutionMapping) {
        final Element s, p, o;
        // Subject
        if (pattern.subject.isVariable()) {
            final int var = pattern.subject.asVariable().varId;
            if (solutionMapping.contains(var)) {
                s = solutionMapping.get(var);
            } else {
                s = pattern.subject;
            }
        } else {
            s = pattern.subject;
        }
        // Predicate
        if (pattern.predicate.isVariable()) {
            final int var = pattern.predicate.asVariable().varId;
            if (solutionMapping.contains(var)) {
                p = solutionMapping.get(var);
            } else {
                p = pattern.predicate;
            }
        } else {
            p = pattern.predicate;
        }
        // Object
        if (pattern.object.isVariable()) {
            final int var = pattern.object.asVariable().varId;
            if (solutionMapping.contains(var)) {
                o = solutionMapping.get(var);
            } else {
                o = pattern.object;
            }
        } else {
            o = pattern.object;
        }

        // TODO
        return new QuadStarPattern(null, s, p, o);
    }
}