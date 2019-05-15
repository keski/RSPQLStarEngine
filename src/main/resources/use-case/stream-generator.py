#!/bin/python3

import random
import re
from datetime import datetime
import sys

def make_tg(identifier, time, sensor, foi, value, confidence, reif):
    
    graph = "_:g{0}".format(identifier)
    observation = "<observation/{0}>".format(identifier)
    timestamp = datetime.utcfromtimestamp(time).strftime('%Y-%m-%dT%H:%M:%S')
    
    payload =  "   {0} a sosa:Observation ;\n".format(observation)
    payload += "      sosa:madeObservation {0} ;\n".format(sensor)
    payload += "      sosa:featureOfInterest {0} ".format(foi)

    if confidence != None:
        payload += ".\n"
        if reif:
            payload += as_reif(observation, "sosa:hasSimpleResult", value)
        else:
            payload += as_rdf_star(observation, "sosa:hasSimpleResult", value)
        payload += "      ex:confidence {0} .".format(confidence)
    else:
        payload += ";\n      sosa:hasSimpleResult {0} .".format(value)


    tg = "_:g{0} {{\n{1}\n}}\n".format(identifier, payload)
    tg += "_:g{0} prov:generatedAtTime \"{1}\"^^xsd:dateTime .\n".format(identifier, timestamp)
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

def main():
    number_of_observations = 3600
    trigger_after = 5

    prefixes = "@base <http://base/> .\n"
    prefixes += "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
    prefixes += "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
    prefixes += "@prefix sosa: <http://www.w3.org/ns/sosa/> .\n"
    prefixes += "@prefix activity: <http://www.example.org/ontology/activity/> .\n"
    prefixes += "@prefix motion: <http://www.example.org/ontology/motion/> .\n"
    prefixes += "@prefix ex: <http://www.example.org/ontology#> .\n"
    prefixes += "@prefix prov: <http://www.w3.org/ns/prov#> .\n"

    for reif in [True, False]:
        random.seed(0)

        # activity, every 30 seconds
        d = "reification/" if reif else "rdfstar/"
        myfile = open(d + "activity.trigs", "w")
        myfile.write(compress(prefixes) + "\n")
        time = 1556617861
        for identifier in range(number_of_observations):
            confidence = 1 - (.1 * random.random())
            tg = make_tg(identifier, time, "<sensor/system>", "<person/1>", "activity:resting", None, reif)
            time += 10
            myfile.write(compress(tg))
            myfile.write("\n")
        myfile.close()


        # heart reate, every second
        d = "reification/" if reif else "rdfstar/"
        myfile = open(d + "heart.trigs", "w")
        myfile.write(compress(prefixes) + "\n")
        time = 1556617861
        base_heart_rate = 85
        for identifier in range(number_of_observations):
            if identifier == trigger_after: base_heart_rate = 100
            confidence = 1 - (.2 * random.random())
            heart_rate = base_heart_rate + 10 * random.random() # base plus 0 to 10 beats per minute
            tg = make_tg(identifier, time, "<sensor/heart_rate/1>", "<person/1>", int(heart_rate), confidence, reif)
            time += 1
            myfile.write(compress(tg))
            myfile.write("\n")
        myfile.close()

        # breathing rate, every second
        d = "reification/" if reif else "rdfstar/"
        myfile = open(d + "breathing.trigs", "w")
        myfile.write(compress(prefixes) + "\n")
        time = 1556617861
        base_breathing_rate = 19
        for identifier in range(number_of_observations):
            if identifier == trigger_after: base_breathing_rate = 10
            confidence = 1 - (.2 * random.random())
            breathing_rate = base_breathing_rate + 1 * random.random() # base plus 0 to 5 breaths per minute
            tg = make_tg(identifier, time, "<sensor/breathing_rate/1>", "<person/1>", int(breathing_rate), confidence, reif)
            time += 1
            myfile.write(compress(tg))
            myfile.write("\n")
        myfile.close()

        # oxygen saturation, every second
        d = "reification/" if reif else "rdfstar/"
        myfile = open(d + "oxygen.trigs", "w")
        myfile.write(compress(prefixes) + "\n")
        time = 1556617861
        oxygen_level = 0.97
        for identifier in range(number_of_observations):
            confidence = 1 - (.2 * random.random())
            oxygen = oxygen_level + 0.02 * random.random() # base plus 0 to 2 %
            tg = make_tg(identifier, time, "<sensor/oxygen/1>", "<person/1>", oxygen, confidence, reif)
            time += 1
            myfile.write(compress(tg))
            myfile.write("\n")
        myfile.close()

        # location, reported every 5 s
        d = "reification/" if reif else "rdfstar/"
        myfile = open(d + "location.trigs", "w")
        myfile.write(compress(prefixes) + "\n")
        time = 1556617861
        location = "<person/1/home>"
        for identifier in range(number_of_observations):
            if identifier > 3: location = "<away>"
            confidence = 1 - (.1 * random.random())
            tg = make_tg(identifier, time, "<sensor/location/2>", "<person/2>", location, None, reif)
            time += 10
            myfile.write(compress(tg))
            myfile.write("\n")
        myfile.close()

def compress(data):
    return re.sub("\s+", " ", data)

main()