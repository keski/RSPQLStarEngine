package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.index.IdBasedTriple;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class HashReferenceDictionary implements ReferenceDictionary {
    final private ArrayList<IdBasedTriple> idToNodeTriple = new ArrayList<>();
    final private ConcurrentHashMap<IdBasedTriple, Long> nodeTripleToId = new ConcurrentHashMap();

    @Override
    public IdBasedTriple getIdBasedTriple(long id) {
        final long body = IdFactory.getReferenceIdBody(id);
        if (body <= idToNodeTriple.size()) {
            return idToNodeTriple.get((int) (body - 1));
        }
        return null;
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
        long body = IdFactory.getReferenceIdBody(id);

        if (body < idToNodeTriple.size()) {
            idToNodeTriple.set((int) body, idBasedTriple); // replace existing value
        } else {
            idToNodeTriple.add(idBasedTriple);
        }
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
    public void print(int limit) {
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
        System.out.println(sb.toString());
    }
}
