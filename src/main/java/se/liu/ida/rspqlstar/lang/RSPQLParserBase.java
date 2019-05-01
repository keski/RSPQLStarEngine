package se.liu.ida.rspqlstar.lang;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.lang.SPARQLParserBase;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;

import java.util.ArrayDeque;
import java.util.Deque;

public class RSPQLParserBase extends SPARQLParserBase {
    private RSPQLStarQuery query;
    private Deque<RSPQLStarQuery> stack = new ArrayDeque();

    public RSPQLParserBase(){}

    @Override
    public void setQuery(Query q) {
        query = new RSPQLStarQuery(q);
        setPrologue(q);
    }

    public void setQuery(RSPQLStarQuery q) {
        query = q ;
        setPrologue(q) ;
    }

    public RSPQLStarQuery getQuery() { return query ; }

    protected void pushQuery() {
        throw new IllegalStateException("Subqueries are currently not supported");
    }
}
