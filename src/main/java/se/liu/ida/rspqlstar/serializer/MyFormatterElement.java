package se.liu.ida.rspqlstar.serializer;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.serializer.FormatterElement;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.syntax.*;
import se.liu.ida.rspqlstar.syntax.ElementNamedWindow;
import se.liu.ida.rspqlstar.util.MyFmtUtils;

/**
 * Based on the FormatterElement but with added support for visiting ElementNamedWindow
 * and to expose the protected serialization context (required for MyFmtUtils) and the
 * slotToString method.
 */

public class MyFormatterElement extends FormatterElement {
    public MyFormatterElement(IndentedWriter out, SerializationContext context) {
        super(out, context);
    }

    public SerializationContext getContext(){
        return context;
    }

    protected String slotToString(Node n){
        return MyFmtUtils.stringForNode(n, context) ;
    }

    // This has been modified to handle both window clauses and graph clauses
    // The alternative is to extend ElementVisitor and also to install an extension
    // of the element walker.
    //@Override
    public void visit(ElementNamedWindow el) {
        visitNodePattern("WINDOW", el.getWindowNameNode(), el.getElement());
    }

    private void visitNodePattern(String label, Node node, Element subElement) {
        int len = label.length();
        out.print(label);
        out.print(" ");
        String nodeStr = (node == null) ? "*" : slotToString(node);
        out.print(nodeStr);
        len += nodeStr.length();
        if ( GRAPH_FIXED_INDENT ) {
            out.incIndent(INDENT);
            out.newline(); // NB and newline
        } else {
            out.print(" ");
            len++;
            out.incIndent(len);
        }
        visitAsGroup(subElement);

        if ( GRAPH_FIXED_INDENT )
            out.decIndent(INDENT);
        else
            out.decIndent(len);
    }

}
