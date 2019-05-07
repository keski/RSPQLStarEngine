package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class HashReferenceDictionary implements ReferenceDictionary {
    final private ConcurrentHashMap<Long, IdBasedTriple> idToNodeTriple = new ConcurrentHashMap();
    final private ConcurrentHashMap<IdBasedTriple, Long> nodeTripleToId = new ConcurrentHashMap();

    @Override
    public IdBasedTriple getIdBasedTriple(long id) {
        return idToNodeTriple.get(id);
    }

    @Override
    public long addIfNecessary(IdBasedTriple idBasedTriple) {
        final Long id = getId(idBasedTriple);
        if (id != null) {
            return id;
        }
        return addNode(idBasedTriple);
    }

    public long addNode(IdBasedTriple idBasedTriple){
        long id = IdFactory.nextReferenceKeyId();
        idToNodeTriple.put(id, idBasedTriple);
        nodeTripleToId.put(idBasedTriple, id);
        return id;
    }

    @Override
    public Long getId(IdBasedTriple idBasedTriple) {
        return nodeTripleToId.get(idBasedTriple);
    }

    @Override
    public long size() {
        return nodeTripleToId.size();
    }

    @Override
    public void print(PrintStream out, int limit) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Reference Dictionary\n");

        if (idToNodeTriple.size() == 0) {
            sb.append(">>> empty <<<\n");
        }
        for (int i = 1; i <= idToNodeTriple.size() && i < limit; i++) {
            final IdBasedTriple node = getIdBasedTriple(i);
            long id = i + IdFactory.REFERENCE_BIT;
            sb.append(String.format("%s (id: %s) : %s\n", i, id, node));
        }
        out.println(sb.toString());
    }

    @Override
    public void clear(){
        idToNodeTriple.clear();
        nodeTripleToId.clear();
    }
}
