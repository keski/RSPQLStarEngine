#!/bin/python3

import random
import re
from datetime import datetime
import sys

def make_tg(x, triples, f):
    
    tab = "      "

    # All statements are associated with a confidence value
    payload = ""
    for i in range(triples):
        if f == "reification":
            payload += as_reif("<observation/{0}>".format(i), "<value/{0}>".format(i), x)
        else:
            payload += as_rdf_star("<observation/{0}>".format(x), "<value/{0}>".format(i), "<value/{0}>".format(i))
        payload += "{0}<other/meta> \"{1}\"^^xsd:float .\n".format(tab, 0)

    # Add the value of "interest" and confidence for this value
    if f == "reification":
        payload += as_reif("<observation/{0}>".format(x), "<value>", x)
    else:
        payload += as_rdf_star("<observation/{0}>".format(x), "<value>", "<value/{0}>".format(x))
    payload += "{0}<confidence> \"{1}\"^^xsd:float .".format(tab, random.random())


    tg = "_:g{0} {{\n{1}\n}}".format(x, payload)
    return tg

def as_reif(s, p, o):
    reif =  "   {0} {1} {2} .\n".format(s, p, o);
    reif += "   [] a rdf:Statement ;\n"
    reif += "      rdf:subject {0} ;\n".format(s)
    reif += "      rdf:predicate {0} ;\n".format(p)
    reif += "      rdf:object {0} ;\n".format(o)
    return reif

def as_rdf_star(s, p, o):
    return "   <<{0} {1} {2}>>\n".format(s, p, o)

def generate_streams():
    prefixes = "@base <http://base/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . "
    for f in ["reification", "rdfstar"]:
        for meta in range(11):
            random.seed(0)
            myfile = open("{0}-meta-{1}.trigs".format(meta+1, f), "w")
            myfile.write(prefixes + "\n")
            for x in range(1000):
                tg = make_tg(x, meta, f)
                #print(tg)
                myfile.write(compress(tg))
                myfile.write("\n")
            myfile.close()

def compare_size():
    myfile = open("overhead_compared.txt", "w")
    prefixes = "@base <http://base/> . @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . @prefix xsd: <http://www.w3.org/2001/XMLSchema#> . "
    for f in ["reification", "rdfstar"]:
        myfile.write(f + "\n")
        for meta in range(0, 31, 2):
            random.seed(0)
            tg = make_tg(0, meta, f)
            compressed_tg = compress(prefixes + "\n" + tg)
            myfile.write("({0},{1}) ".format(meta, utf8len(compressed_tg)/1024.0))
        myfile.write("\n")
    myfile.close()

def compress(data):
    return re.sub("\s+", " ", data)

def utf8len(s):
    return len(s.encode('utf-8'))

generate_streams()
compare_size()