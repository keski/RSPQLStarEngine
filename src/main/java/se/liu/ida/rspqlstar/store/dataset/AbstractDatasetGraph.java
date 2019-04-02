package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraphBase;

import java.util.Iterator;

abstract class AbstractDatasetGraph extends DatasetGraphBase {

    @Override
    public Graph getDefaultGraph() {
        throw new NotImplementedException("DatasetStarGraph.getDefaultGraph");
    }

    @Override
    public org.apache.jena.graph.Graph getGraph(Node node) {
        throw new NotImplementedException("DatasetStarGraph.getGraph");
    }

    @Override
    public void addGraph(Node node, Graph graph) {
        throw new NotImplementedException("DatasetStarGraph.addGraph");
    }

    @Override
    public void removeGraph(Node node) {
        throw new NotImplementedException("DatasetStarGraph.removeGraph");
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        throw new NotImplementedException("DatasetStarGraph.listGraphNodes");
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public void begin(TxnType txnType) {
        throw new NotImplementedException("DatasetStarGraph.begin");
    }

    @Override
    public void begin(ReadWrite readWrite) {
        throw new NotImplementedException("DatasetStarGraph.begin");
    }

    @Override
    public boolean promote(Promote promote) {
        return false;
    }

    @Override
    public void commit() {
        throw new NotImplementedException("DatasetStarGraph.commit");
    }

    @Override
    public void abort() {
        throw new NotImplementedException("DatasetStarGraph.abort");
    }

    @Override
    public void end() {
        throw new NotImplementedException("DatasetStarGraph.end");
    }

    @Override
    public ReadWrite transactionMode() {
        throw new NotImplementedException("DatasetStarGraph.transactionMode");
    }

    @Override
    public TxnType transactionType() {
        throw new NotImplementedException("DatasetStarGraph.transactionType");
    }

    @Override
    public boolean isInTransaction() {
        throw new NotImplementedException("DatasetStarGraph.isInTransaction");
    }
}
