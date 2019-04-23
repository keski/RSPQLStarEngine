package se.liu.ida.rspqlstar.syntax;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.syntax.Element;
import org.apache.jena.sparql.syntax.ElementService;
import org.apache.jena.sparql.syntax.ElementVisitor;
import org.apache.jena.sparql.util.NodeIsomorphismMap;

public class ElementNamedWindow extends Element {
    private Node windowNameNode ;
    private Element element ;

    public ElementNamedWindow(Node windowNameNode, Element el) {
        this.windowNameNode = windowNameNode;
        element = el;
    }

    public Node getWindowNameNode() {
        return windowNameNode;
    }

    public Element getElement() {
        return element;
    }

    @Override
    public int hashCode() {
        return element.hashCode() ^ windowNameNode.hashCode();
    }

    @Override
    public boolean equalTo(Element el2, NodeIsomorphismMap isoMap) {
        if (el2 == null) return false;

        if (!(el2 instanceof ElementNamedWindow))
            return false;

        ElementNamedWindow g2 = (ElementNamedWindow) el2;
        if (!getWindowNameNode().equals(g2.getWindowNameNode()))
            return false;
        if (!getElement().equalTo(g2.getElement(), isoMap))
            return false;
        return true;
    }

    @Override
    public void visit(ElementVisitor v) {
        //v.visit(this);
    }
}


