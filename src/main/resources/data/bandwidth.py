#!/bin/python3

import random
import re
from datetime import datetime
import sys

def make_example(num_payload, num_meta):
    data = ""
    for i in range(num_payload):
        if num_meta == 0:
            data += "\n   <observation/{0}> <has/data/value> <data/value#{0}> ".format(i)
        else:
            data += "\n   <<<observation/{0}> <has/data/value> <data/value#{0}>>> ".format(i)
        
        for j in range(num_meta):
            data += "\n      <has/meta/data/prop#{0}> <meta/data/value#{0}> ".format(j)
            if j == num_meta - 1:
                data += ".\n"
            else:
                data += ";"

    t = 1556617861 
    time = datetime.utcfromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%S')
    return "\n_:{0} {{ {1}}}\n_:{0} <time/property> {2} .".format(0, data, t)

def make_reif_example(num_payload, num_meta):
    data = ""
    for i in range(num_payload):
        data += "\n   <observation/{0}> <has/data/value> <data/value#{0}> ".format(i)
        if num_meta > 0:
            data += "\n   _:b{0} a rdf:Statement ;".format(i)
            data += "\n          rdf:subject <observation/{0}> ;".format(i)
            data += "\n          rdf:predicate <has/data/value> ;"
            data += "\n          rdf:object <data/value#{0}>;".format(i)
        for j in range(num_meta):
            data += "\n          <has/meta/data/prop#{0}> <meta/data/value#{0}> ".format(j)
            if j == num_meta - 1:
                data += ".\n"
            else:
                data += ";"

    t = 1556617861 
    time = datetime.utcfromtimestamp(t).strftime('%Y-%m-%dT%H:%M:%S')
    return """\n_:{0} {{ {1}}}\n_:{0} <time/property> {2} .""".format(0, data, t)

def main():
    prefixes = "@base <http://base/> .\n@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> ."

    # fixed payload, varying meta
    f = open("1-payload-x-meta.csv","w")
    f.write("# payload;# meta;RDF* (bytes);Reification (bytes)\n")
    payload = 1
    for num_meta in range(31):
        # RDF* stream element
        data1 = prefixes + "\n" + make_example(payload, num_meta)
        print(data1)
        bytes1 = sys.getsizeof(compress(data1))
        # RDF reification element
        data2 = prefixes + "\n" + make_reif_example(payload, num_meta)
        print(data2)
        bytes2 = sys.getsizeof(compress(data2))
        f.write("{0};{1};{2};{3}\n".format(payload, num_meta, bytes1, bytes2))
    
    f.close()

    # fixed meta, varying payload
    f = open("x-payload-1-meta.csv","w")
    f.write("# payload;# meta;RDF* (bytes);Reification (bytes)\n")
    num_meta = 1
    for payload in range(1,31):
        # RDF* stream element
        data1 = prefixes + "\n" + make_example(payload, num_meta)
        bytes1 = sys.getsizeof(compress(data1))
        # RDF reification element
        data2 = prefixes + "\n" + make_reif_example(payload, num_meta)
        bytes2 = sys.getsizeof(compress(data2))
        f.write("{0};{1};{2};{3}\n".format(payload, num_meta, bytes1, bytes2))

    f.close()

def compress(data):
    return re.sub("\s+", " ", data)

main()



