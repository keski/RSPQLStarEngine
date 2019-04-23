package se.liu.ida.rspqlstar.lang;

import org.apache.jena.query.Query;
import org.apache.jena.sparql.lang.SPARQLParserBase;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;

public class RSPQLParserBase extends SPARQLParserBase {
    private RSPQLStarQuery query;

    public RSPQLParserBase(){}

    @Override
    public void setQuery(Query q) {
        query = new RSPQLStarQuery(q);
        setPrologue(q) ;
        super.query = null;
    }

    public void setQuery(RSPQLStarQuery q) {
        query = q ;
        setPrologue(q) ;
    }

    public RSPQLStarQuery getQuery() { return query ; }
}
