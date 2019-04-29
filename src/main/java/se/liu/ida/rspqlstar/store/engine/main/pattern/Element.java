package se.liu.ida.rspqlstar.store.engine.main.pattern;

/**
 * Defines the elements that appear as subject, predicates and objects in TripleStarPattern.
 */
public class Element {

    public boolean isKey(){
        return false;
    }

    public Key asKey() {
        return (Key) this;
    }

    public boolean isVariable(){
        return false;
    }

    public Variable asVariable(){
        return (Variable) this;
    }

    /**
     * An element is considered to be concrete if it is not a variable.
     * @return
     */
    public boolean isConcrete(){
        return !isVariable();
    }
}
