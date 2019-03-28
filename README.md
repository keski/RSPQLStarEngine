# Star Triple Store

This document describes the current state of the Star Triple Store as of July 14, 2018, and clarifies important terms and design decisions. The software is a triple store based on a vision by Olaf Hartig, where he proposed logical extensions to RDF and SPARQL, RDF\* and SPARQL\*, to create a "better" way to represent and refer to statement level metadata. The triple store is based primarily on Apache Jena 3.7.0.

## Extensions to RDF and SPARQL

The basic idea of RDF\* is that a triple can contain an embedded triple in the subject or object position, e.g.:

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

SPARQL\* additionally adds the possibility to use the `BIND` keyword to bind a triple to a variable. This means that the previous query could be re-written as:

```
PREFIX : <http://example.org#>
SELECT ?president
WHERE {
   BIND(<<?president a :president>> AS ?t)
   ?t :trueInYear ?year
}
```

## Dictionaries
Each node of a triple (subject, predicate or object) is encoded using a numeric (64 bit) ID. This allows operations to be conducted on the IDs directly, rather than on the node objects themselves, and is leveraged to encode *embedded triples* as a single ID.

### Node dictionary
The mappings from an ID to a node (and vice versa) is stored in a `NodeDictionary`. To denote that a node has a certain ID below, we use the following notation:

```node -> id```

where the ID is represented in base 10 for readability. The following example shows that the URI `:obama` is encoded with the ID 5:

```:obama -> 5```

### Reference dictionary
The `ReferenceDictionary` encodes a special type of ID, mapping from a reference ID to a triple. The use of these IDs are described further in the encoding subsection. The notation is as follows:
 
```
reference ID -> (subject ID, predicate ID, object ID)
```

### Variable dictionary
The `VariableDictionary` encodes the variables of the triple patterns as integers. Having this numeric representation of variables makes the typical comparison cost lower than the default Jena implementation. 

# Encoding
Each node ID is represented using a 64 bit integer (long). Additionally, there are three distinct types of IDs, indicated by the most significant bit (MSB) of the ID. For the remainder of this section we will therefore use the binary notation for IDs.

## Simple ID
Simple IDs are used to represent URIs, literals, and blank nodes. The first MSB is always set to zero and the remaining 63 bits hold the identifier of the node, which is used to reference the node in the `NodeDictionary`. As an example, if the URI `:obama` is given the ID 5, the encoded ID would be:

```
0-000000000000000000000000000000000000000000000000000000000000101
```

## Embedded ID
If the two first MSBs are set to `10` the ID represents an embedded triple. For example, assume that we configure the system to use 2 bits for the header, 20 bits for the subject, 20 bits for the predicate, and 22 bits for the object. Now, assume that we have the following node dictionary:

```
:obama -> 5 
a -> 2 
:president -> 3
:trueInYear -> 4
"2015"^^xsd:integer -> 6
```

and want to encode the following statement:

```
<<:obama a :president>> :trueInYear "2015"^^xsd:integer
```

The embedded triple part in this statement would be encoded as follows (dashes included for readability):

```
10-00000000000000000101-00000000000000000010-0000000000000000000011
```

and the full triple would be represented as:

```
10-00000000000000000101000000000000000000100000000000000000000011
0-000000000000000000000000000000000000000000000000000000000000100
0-000000000000000000000000000000000000000000000000000000000000110
```

The embedded triple is thus created by shifting the subject, predicate, and object IDs to their allocated bit space and combining them using a bitwise OR with, together with the appropriate header. The parts of the embedded ID can similarly be extracted from the ID without requiring any additional lookups in the node dictionary or triple indexes.

## Reference ID
If the two MSBs are set to `11`, an ID represents a reference triple. A reference ID represents a triple node that cannot be encoded as an embedded ID. A reference ID is required when at least one node does not fit within its allocated bit space within an embedded ID. For example, if the subject position supports only 20 bits but the subject that is to be encoded is represended using 21 bits. This is always the case for embedded triples that contain other embedded triples (i.e., nested embedded triples), since it requires 64 bits to be encoded.

Imagine the following embedded node:

```
<<:obama a :president>>
```

With the identifiers as follows and the same bit space partioning as in the previous example:
```
obama -> 2097152
a -> 2
:president -> 3
```

Since `2097152` requires 21 bits, a refrence ID will be created as follows:

```
11-000000000000000000000000000000000000000000000000000000000001
```

This ID is added to the node dictionary with its corresponding triple node object. In order to decode a reference ID this means that an additional lookup in the node dictionary is necessary.


# Index 
The system contains three indexes: `SPO`, `POS`, and `OSP`. `SPO` answers triple patterns like `SP* and S**`. `OSP` answers triple patterns like `OS* and O**`. `POS` answers triple patterns like `PO* and P**`. An index lookup returns an iterator over the matching triples. When indexing a triple containing an embedded triple, the embedde triple is also indexed separately. The software currently support three different implementation of the underlying index:

- `ChronicleMapIndex` Uses a memory mapped hash map to avoid GC and increase the maximum supported memory.
- `FlatIndex` Uses a single hash map to store all triple indexes.
- `HashIndex` Uses a nested hash map structure.
- `PrimitiveIndex` Uses separate maps for IDs that can be represented using integers or longs for the first two fields.
- `TreeIndex` Uses a tree map to sacrifice speed for potential memory saving.

Initial tests indicate that the `FlatIndex` provides the best balance between speed and memory requirements, especially for large datasets.

# Query optimization
The Apache Jena execution engine uses iterators when evaluating triple patterns, and for each triple pattern evaluation the resulting solution mapping is passed to the next query operator. The Star Triple Store uses the selectivity of triple patterns to order to optimize this process. The selectivity estimation is based on the heuristics presented in [Heuristics-based Query Optimisation for SPARQL][1], but has been extended for the SPARQL\* `BIND` pattern. As a general guideline, the more variables a triple pattern contains, the less selective it is. For example, the triple pattern `(s,p,?)` is more selective than `(s,?,?)`. If two triple patterns have an equal number of variables, the following ordering principle applies:

```
o > s > p
```

The logic behind this is that there are typically relatively few distinct predicates, and a predicate will thus match a large number of triples. Following this logic, objects are more selective than subjects, because several triples often share a subject, while objects tend to be unique.
The order of all triple patterns, in terms of selectivity, in the Star Triple Store is as follows:

```
(s,p,o)
(s,p,o) as ?
(s,?,o)
(s,?,o) as ?
(?,p,o)
(?,p,o) as ?
(s,p,?)
(s,p,?) as ?
(?,?,o)
(?,?,o) as ?
(s,?,?)
(s,?,?) as ?
(?,p,?)
(?,p,?) as ?
(?,?,?)
(?,?,?) as ?
```

[1]: https://dl.acm.org/citation.cfm?id=2247635

In the case where triple patterns are tied in selectivity, the known solution mappings are used. If a solution mapping is a reference key, we break the tie in the favor of the triple patterns without a reference key. The reason for this is that there is an overhead associated with looking up reference keys in the dicitonary.

# Node dictionary optimization
To maximize the amount of triples that can be represented using embedded IDs, and to minimize the production of reference IDs, the way in which node IDs are generated is of critical importance. If we simply produce new IDs incrementally, the bit space will be shared by all bit partitions. For example, if we know that our data only contains 5 distinct predicates that are used within embedded triples, allocating 3 bits for the predicate should be sufficient, and we can allocated 28 bits for the subject and 29 bits for the object. Now, assume that we are generating IDs for the following data set:

```
:obama a :person .
:michelle a :person .
:sasha a :person .
<<:obama :birthPlace :hawaii>> :extractionSource :wikipedia .
<<:obama a :president>> :trueInYear "2015"^^xsd:integer .
<<:obama :fatherOf :sasha>> :extractionSource :wikipedia . 
<<:michelle :motherOf :sasha>> :extractionSource :wikipedia .
<<:obama :marriedTo :michelle>> :extractionSource :wikipedia .
<<:obama :marriedTo :michelle>> :extractionSource :wikipedia .
```

If node IDs are generated incrementally we would expect the following node dictionary:

```
:obama -> 1
a -> 2 
:person -> 3
:michelle -> 4
:sasha -> 5
:birthPlace -> 6
:hawaii -> 7
:extractionSource -> 8
:wikipedia -> 9
:president -> 10
:trueInYear -> 11
"2015"^^xsd:integer -> 12
:fatherOf -> 13
:motherOf -> 14
:marriedTo -> 15
```

Notably, the predicates `:fatherOf`, `:motherOf` and `:marriedTo` are all represented by more than 3 bits, and the corresponding triples cannot be represented using embedded IDs. Several strategies can be employed to allocate new IDs more efficiently. The Star Triple Store supports an optional ID generation optimization strategy, requiring a extra pass over the data prior to indexing. The optimizer generates IDs only for the embedded triples, and prioritizes ID generation with respect to the size of the allocated bits. Using the optimizer, the following node dictonary would be generated instead:
  
```
:birthPlace -> 1
a -> 2
:fatherOf -> 3
:motherOf -> 4
:marriedTo -> 5
:obama -> 6
:michelle -> 7
:hawaii -> 8
:president -> 9
:sasha -> 10
:extractionSource -> 11
:wikipedia -> 12
:trueInYear -> 13
"2015"^^xsd:integer -> 14
:person -> 15
```

The optimized node dictionary allows all embedded triples in the dataset to be represented using embedded IDs. However, if the required bit size for a field grows beyond the configured partion size, the optimizer will ignore it.

