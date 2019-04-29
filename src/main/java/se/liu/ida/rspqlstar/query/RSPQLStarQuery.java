package se.liu.ida.rspqlstar.query;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryVisitor;
import org.apache.jena.query.Syntax;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.serializer.QuerySerializerFactory;
import org.apache.jena.sparql.serializer.SerializerRegistry;
import org.apache.jena.sparql.syntax.PatternVars;
import se.liu.ida.rspqlstar.lang.NamedWindow;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.serializer.RSPQLStarQueryVisitor;
import se.liu.ida.rspqlstar.syntax.MyPatternVars;

import java.time.Duration;
import java.util.*;

public class RSPQLStarQuery extends Query {
    private String outputStream = null;
    private Duration computedEvery = null;
    private Map<String, NamedWindow> namedWindows = new HashMap<>();


    public RSPQLStarQuery(Query query){
        super(query);
        setSyntax(RSPQLStar.syntax);
    }

    public List<String> getResultVars() {
        if(isQueryResultStar())
            findAndAddEmbeddedNamedVars();
        return Var.varNames(this.projectVars.getVars());
    }

    public void setOutputStream(String outputStream){
        this.outputStream = outputStream;
    }

    public void setComputedEvery(String interval){
        computedEvery = stringAsDuration(interval);
    }

    public String getOutputStream() {
        return outputStream;
    }

    public Duration getComputedEvery() {
        return computedEvery;
    }

    public Map<String, NamedWindow> getNamedWindows(){
        return namedWindows;
    }

    public void addNamedWindow(String windowIri, String streamIri, String range, String step){
        final NamedWindow window = new NamedWindow(windowIri, streamIri, stringAsDuration(range), stringAsDuration(step));
        namedWindows.put(windowIri, window);
    }

    public Duration stringAsDuration(String duration){
        return Duration.parse(duration);
    }

    public void visit(QueryVisitor v) // extend
    {
        RSPQLStarQueryVisitor visitor = (RSPQLStarQueryVisitor) v;
        visitor.startVisit(this) ;
        visitor.visitResultForm(this) ;
        visitor.visitPrologue(this) ;
        visitor.visitRegisterForm(this);
        visitor.visitComputedEveryForm(this);
        if ( this.isSelectType() )
            visitor.visitSelectResultForm(this) ;
        if ( this.isConstructType() )
            visitor.visitConstructResultForm(this) ;
        if ( this.isDescribeType() )
            visitor.visitDescribeResultForm(this) ;
        if ( this.isAskType() )
            visitor.visitAskResultForm(this) ;
        if ( this.isJsonType() )
            visitor.visitJsonResultForm(this) ;
        visitor.visitDatasetDecl(this) ;
        visitor.visitQueryPattern(this) ;
        visitor.visitGroupBy(this) ;
        visitor.visitHaving(this) ;
        visitor.visitOrderBy(this) ;
        visitor.visitOffset(this) ;
        visitor.visitLimit(this) ;
        visitor.visitValues(this) ;
        visitor.finishVisit(this) ;
    }

    public void serialize(IndentedWriter writer, Syntax outSyntax) {
        // Try to use a serializer factory if available
        QuerySerializerFactory factory = SerializerRegistry.get().getQuerySerializerFactory(outSyntax);
        QueryVisitor serializer = factory.create(outSyntax, this, writer);
        visit(serializer);
    }

    @Override
    public List<Var> getValuesVariables() {
        return this.valuesDataBlock == null ? null : this.valuesDataBlock.getVars();
    }

    public void findAndAddEmbeddedNamedVars() {
        for(Var var : MyPatternVars.vars(new ArrayList<>(), this.getQueryPattern())){
            addResultVar(var);
        }
        // add window ref explicitly!
    }
}
