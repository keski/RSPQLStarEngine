package se.liu.ida.rspqlstar.store.dictionary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.jena.sparql.core.Var;

import se.liu.ida.rspqlstar.store.engine.main.pattern.Variable;

/**
 * This dictionary creates and contains the mappings from a jena Var {@link Var} to an integer.
 * This integer is used to represent and compare variables.
 * <p>
 * It is implemented as a singleton class. Correct variable scoping has not been verified.
 */
public class VarDictionary {
    static private ArrayList<Var> dictId2Var = new ArrayList<>();
    static private Map<String, Integer> dictVarName2Id = new HashMap<>();
    static private int freshVariableCounter = 1;

    static private VarDictionary instance;

    static public VarDictionary get() {
        if (instance == null) {
            instance = new VarDictionary();
        }
        return instance;
    }

    static public void reset(){
        dictId2Var.clear();
        dictVarName2Id.clear();
        freshVariableCounter = 1;
    }

    /**
     * Returns the query variable identified by the given identifier.
     *
     * @throws IllegalArgumentException if the given identifier is unknown to
     *                                  this dictionary
     */
    public Var getVar(int id) throws IllegalArgumentException {
        final Var v = dictId2Var.get(id);

        if (v == null) {
            throw new IllegalArgumentException("The given identifier (" + String.valueOf(id) + ") is unknown.");
        }

        return v;
    }

    /**
     * Returns the identifier that identifies the given query variable.
     *
     * @throws IllegalArgumentException if the given variable is unknown to
     *                                  this dictionary
     */
    public int getId(Var v) throws IllegalArgumentException {
        final Integer i = dictVarName2Id.get(v.getVarName());
        if (i == null) {
            throw new IllegalArgumentException("The given variable (" + v.getVarName() + ") is unknown.");
        }

        return i.intValue();
    }

    /**
     * Returns the number of query variables known by this dictionary.
     */
    final public int size() {
        return dictId2Var.size();
    }

    // operations

    /**
     * Returns an identifier that identifies the given query variable.
     * If there is no identifier for the given query variable yet this method
     * creates a new identifier and adds it to the dictionary.
     */
    final public int createId(Var v) {
        final int result;
        final Integer i = dictVarName2Id.get(v.getVarName());

        if (i == null) {
            result = dictId2Var.size();
            dictId2Var.add(v);
            assert result < Integer.MAX_VALUE;
            dictVarName2Id.put(v.getVarName(), Integer.valueOf(result));
        } else {
            result = i.intValue();
        }

        return result;
    }

    public Variable createVariable(Var v) {
        return new Variable(createId(v));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < dictId2Var.size(); i++) {
            sb.append("#" + i + ": " + dictId2Var.get(i) + "\n");
        }
        return sb.toString();
    }

    public void clear() {
        dictId2Var.clear();
        dictVarName2Id.clear();
    }

    public Var getFreshVariable() {
        final Var var = Var.alloc("_" + freshVariableCounter);
        createId(var);
        freshVariableCounter++;
        return var;
    }
}
