package se.liu.ida.rspqlstar.store.dictionary.referencedictionary;

import se.liu.ida.rspqlstar.store.triple.IdBasedQuad;
import se.liu.ida.rspqlstar.store.triple.IdFactory;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class HashReferenceDictionary implements ReferenceDictionary {
    final private ArrayList<IdBasedQuad> idToNodeQuad = new ArrayList<>();
    final private ConcurrentHashMap<IdBasedQuad, Long> nodeQuadToId = new ConcurrentHashMap();

    @Override
    public IdBasedQuad getIdBasedQuad(long id) {
        final long body = IdFactory.getReferenceIdBody(id);
        if (body <= idToNodeQuad.size()) {
            return idToNodeQuad.get((int) (body - 1));
        }
        return null;
    }

    @Override
    public long addIfNecessary(IdBasedQuad idBasedQuad) {
        final Long id = getId(idBasedQuad);
        if (id != null) {
            return id;
        }
        return addNode(idBasedQuad);
    }

    public long addNode(IdBasedQuad idBasedQuad){
        long id = IdFactory.nextReferenceKeyId();
        long body = IdFactory.getReferenceIdBody(id);

        if (body < idToNodeQuad.size()) {
            idToNodeQuad.set((int) body, idBasedQuad); // replace existing value
        } else {
            idToNodeQuad.add(idBasedQuad);
        }
        nodeQuadToId.put(idBasedQuad, id);
        return id;
    }

    @Override
    public Long getId(IdBasedQuad idBasedQuad) {
        return nodeQuadToId.get(idBasedQuad);
    }

    @Override
    public long size() {
        return nodeQuadToId.size();
    }

    @Override
    public void print(int limit) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Reference Dictionary\n");

        if (idToNodeQuad.size() == 0) {
            sb.append(">>> empty <<<\n");
        }
        for (int i = 1; i <= idToNodeQuad.size() && i < limit; i++) {
            final IdBasedQuad node = getIdBasedQuad(i);
            long id = i + IdFactory.REFERENCE_BIT;
            sb.append(String.format("%s (id: %s) : %s\n", i, id, node));
        }

        System.out.println(sb.toString());
    }
}
