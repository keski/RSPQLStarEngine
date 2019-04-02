# RDFStarStore

This document describes the current state of the RDFStarStore as of April, 2019, and
clarifies important terms and design decisions. The software is an RSP-QL engine based on
the RSP-QL semantics but implements RDF\* and SPARQL\* extensions to represent and query
statement-level metadata. The store is based primarily on Apache Jena 3.7.0.

## Extensions to RDF and SPARQL

The basic idea of RDF\* is that a triple can contain an embedded triple in the subject or
object position, e.g.:

```
<<:obama a :president>> :trueInYear "2015"^^xsd:integer
```

Similarly, embedded triples can also be referenced as triple patterns in SPARQL\*, e.g.:

```
PREFIX : <http://example.org#>
SELECT ?president
WHERE {
   <<?president a :president>> :trueInYear ?year
}
```

SPARQL\* additionally adds the possibility to use the `BIND` keyword to bind a triple to
a variable. This means that the previous query could be re-written as:

```
PREFIX : <http://example.org#>
SELECT ?president
WHERE {
   BIND(<<?president a :president>> AS ?t)
   ?t :trueInYear ?year
}
```

### Node dictionary
The mappings from an ID (long) to a node (and vice versa) is stored in a `NodeDictionary`.
All mappings in the node dictionary use simple IDs, which means that the most significant
bit (MSB) of the ID is not set. For example, an ID with value 5 would be encoded as:

```
0-000000000000000000000000000000000000000000000000000000000000101
```

### Reference dictionary
The `ReferenceDictionary` encodes a special type of ID, which maps a reference ID to a triple.
The reference dictionary uses IDs where the most significant (MSB) of the ID is set. For
example, a reference ID with value 5 would be encoded as;
```
1-000000000000000000000000000000000000000000000000000000000000101
```

### Variable dictionary
The `VariableDictionary` encodes the variables of the triple patterns as integers. Having
this numeric representation of variables makes the typical comparison cost lower than the
default Jena implementation.

**Maybe we should use Jenas instead?**

# Indexes
The system contains six indexes: `GSPO`, `GPOS`, `GOSP`, `SPOG`, `POSG`, and `OSPG`. The
software currently uses a `TreeMap` for storing encoded quads in the index, but the
implementation can easily be extended for other types of indexes.

# Decoding
The bulk of the processing of the system is handled by an extension of the Jena execution
environment. For some of the processing, the node IDs have to be decoded into regular
Jena nodes. This is done by manually finding the corresponding values in the respective
dictionaries, or by wrapping the ID based representation in a `DecodingQuadsIterator` or a
`DecodeBindingsIterator`. 
