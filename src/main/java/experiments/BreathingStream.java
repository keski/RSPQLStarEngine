package experiments;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.vocabulary.RDF;
import se.liu.ida.rspqlstar.store.dataset.RDFStream;
import se.liu.ida.rspqlstar.store.dataset.TimestampedGraph;

import java.util.Date;

public class BreathingStream extends Stream {

    /**
     * @param rdfStream
     * @param totalDelay
     */
    public BreathingStream(RDFStream rdfStream, long totalDelay) {
        super(rdfStream, totalDelay);
    }

    @Override
    public void run() {
        int i = 0;
        while(true){
            TimestampedGraph timestampedGraph = new TimestampedGraph(new Date());
            Node g = ResourceFactory.createResource().asNode();

            Node Sensor = ResourceFactory.createResource("BreathingSensor").asNode();
            Node sensor1 = ResourceFactory.createResource("sensor1").asNode();

            Node hasTime = ResourceFactory.createProperty("hasTime").asNode();
            Node hasValue = ResourceFactory.createProperty("hasValue").asNode();
            Node hasConfidence = ResourceFactory.createProperty("hasConfidence").asNode();

            Node time = ResourceFactory.createTypedLiteral(new Date()).asNode();
            Node confidence = ResourceFactory.createTypedLiteral("0.9", XSDDatatype.XSDfloat).asNode();
            Node value = ResourceFactory.createTypedLiteral("" + i++, XSDDatatype.XSDint).asNode();

            // Add quads
            Quad q1 = new Quad(g, sensor1, RDF.type.asNode(), Sensor);
            Quad q2 = new Quad(g, sensor1, hasValue, value);
            Quad q3 = new Quad(g, new Node_Triple(q2.asTriple()), hasConfidence, confidence);
            Quad q4 = new Quad(Quad.defaultGraphNodeGenerated, g, hasTime, time);
            timestampedGraph
                    .addQuad(q1)
                    .addQuad(q2)
                    .addQuad(q3)
                    .addQuad(q4);
            delayedPush(timestampedGraph, totalDelay);
        }
    }
}
