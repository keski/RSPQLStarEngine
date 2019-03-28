package se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.shared.PrefixMapping;

public class Node_Literal_WithID extends Node_Concrete_WithID {

    public Node_Literal_WithID(LiteralLabel label, long id) {
        super(label, id);
    }

    @Override
    public LiteralLabel getLiteral() {
        return (LiteralLabel) label;
    }

    @Override
    public Object getLiteralValue() {
        return getLiteral().getValue();
    }

    @Override
    public String getLiteralLexicalForm() {
        return getLiteral().getLexicalForm();
    }

    @Override
    public String getLiteralLanguage() {
        return getLiteral().language();
    }

    @Override
    public String getLiteralDatatypeURI() {
        return getLiteral().getDatatypeURI();
    }

    @Override
    public RDFDatatype getLiteralDatatype() {
        return getLiteral().getDatatype();
    }

    @Override
    public boolean getLiteralIsXML() {
        return getLiteral().isXML();
    }

    @Override
    public String toString(PrefixMapping pm, boolean quoting) {
        return ((LiteralLabel) label).toString(quoting);
    }

    @Override
    public boolean isLiteral() {
        return true;
    }

    @Override
    public Object getIndexingValue() {
        return getLiteral().getIndexingValue();
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        } else if (other instanceof Node_Literal_WithID) {
            return getLiteral().equals(((Node_Literal_WithID) other).getLiteral());
        } else  {
            // Warning: Not a symmetric relation.
            return asJenaNode().equals(other);
        }
    }

    /**
     * Test that two nodes are semantically equivalent.
     * In some cases this may be the same as equals, in others
     * equals is stricter. For example, two xsd:int literals with
     * the same value but different language tag are semantically
     * equivalent but distinguished by the java equality function
     * in order to support round tripping.
     */
    @Override
    public boolean sameValueAs(Object o) {
        return asJenaNode().sameValueAs(o);
    }

    @Override
    public boolean matches(Node x) {
        return sameValueAs(x);
    }

    @Override
    public Node asJenaNode() {
        return NodeFactory.createLiteral(getLiteral());
    }
}
