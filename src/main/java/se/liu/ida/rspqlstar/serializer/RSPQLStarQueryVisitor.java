package se.liu.ida.rspqlstar.serializer;

import org.apache.jena.query.QueryVisitor;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;

public interface RSPQLStarQueryVisitor extends QueryVisitor {
    void visitRegisterForm(RSPQLStarQuery query);
    void visitComputedEveryForm(RSPQLStarQuery query);
}
