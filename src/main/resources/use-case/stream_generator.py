#!/bin/python3

import random
import re
from datetime import datetime
# Note that all annotations supported by SSN require the original value to be wrapped
# as an instance. I.e. statement-level meta data is not supported.

heart_rate_level = {
    "normal" : 70,
    "elevated" : 140
}

breathing_rate_level = {
    "normal" : 20,
    "elevated" : 40
}

def make_observation(data, use_reification=False):
    time = datetime.utcfromtimestamp(data["ref_time"] + data["i"]).strftime('%Y-%m-%dT%H:%M:%S')
    confidence = 1.0 - (random.random() * 0.2)

    if use_reification: meta = make_meta_reif(data["observation"], data["value"], data["i"], confidence)
    else: meta = make_meta_rdfstar(data["observation"], data["value"], confidence)
    
    tg = make_tg(data["i"], data["observation"], data["observed_property"], data["sensor"], time, meta)
    return tg

def heart_rate(i, value, t, use_reification=False):
    time = datetime.utcfromtimestamp(t + i).strftime('%Y-%m-%dT%H:%M:%S')
    confidence = 1.0 - (random.random() * 0.2)
    observation = "<observation/heart_rate/{0}>".format(i)

    if use_reification:
        meta = make_meta_reif(observation, value, i, confidence)
    else:
        meta = make_meta_rdfstar(observation, value, confidence)
    
    tg = make_tg(i, observation, "<person1/heart_rate/>", "<sensor1>", time, meta)
    return tg

def activity(i, value, t, use_reification=False):
    time = datetime.utcfromtimestamp(t + i).strftime('%Y-%m-%dT%H:%M:%S')
    confidence = 1.0 - (random.random() * 0.2)
    observation = "<observation/activity/{0}>".format(i)

    if use_reification:
        meta = make_meta_reif(observation, value, i, confidence)
    else:
        meta = make_meta_rdfstar(observation, value, confidence)
    
    tg = make_tg(i, observation, "<person1/activity>", "<sensor1>", time, meta)
    return tg

def breathing(i, value, time, observation_id, observed_property, sensor, use_reification=False):
    time = datetime.utcfromtimestamp(t + i).strftime('%Y-%m-%dT%H:%M:%S')
    confidence = 1.0 - (random.random() * 0.2)
    test = """
        _:{0} {{
            <observation/{0}> a sosa:Observation ;
                sosa:observedProperty <person1/breathing> ;
                sosa:hasFeatureOfInterest <person1> ;
                sosa:madeBySensor <sensor3> ;
                sosa:resultTime "{1}"^^xsd:dateTime .
            <<<observation/{0}> sosa:hasSimpleResult {2}>>  ex:confidence {3} .
        }} .
        _:{0} prov:generatedAtTime "{1}"^^xsd:dateTime .""".format(i, time, value, confidence)
    return test

def make_tg(i, observation, observed_property, sensor, time, meta):
    tg = "_:{0} {{\n".format(i)
    tg += "   {0} a sosa:Observation ;\n".format(observation)
    tg += "      sosa:observedProperty {0} ;\n".format(observed_property)
    tg += "      sosa:hasFeatureOfInterest <person1> ;\n"
    tg += "      sosa:madeBySensor {0} ;\n".format(sensor)
    tg += "      sosa:resultTime \"{0}\"^^xsd:dateTime ".format(time)
    tg += "{0}\n}} .\n".format(meta)
    tg += "_:{0} prov:generatedAtTime \"{1}\"^^xsd:dateTime .""".format(i, time)
    return tg

def make_meta_reif(observation, value, i, confidence):
    meta  = ";\n      sosa:hasSimpleResult {1} .\n".format(observation, value)
    meta += "   _:b{0} a rdf:Statement ;\n".format(i)
    meta += "      rdf:subject {0} ;\n".format(observation)
    meta += "      rdf:predicate sosa:hasSimpleResult ;\n"
    meta += "      rdf:object {0} ;\n".format(value)
    meta += "      ex:confidence {0} .".format(confidence)
    return meta

def make_meta_rdfstar(observation, value, confidence):
    meta = ".\n   <<{0} sosa:hasSimpleResult {1}>>  ex:confidence {2} .".format(observation, value, confidence)
    return meta


def make_heart_rate_stream(prefixes, time, use_reification):
    file_name = "heart_rate.trigs"
    if use_reification:
        file_name = "heart_rate_reif.trigs"
    print("Generate stream: " + file_name)
    out = open(file_name, "w")
    out.write(re.sub(r"\s+", " ", prefixes))
    out.write("\n")

    value = heart_rate_level["normal"]
    for i in range(60*10): # 10 min
        # Set actvity to resting after 30 sec
        if i == 30: heart_rate_level["elevated"]
 
        data = {
            "i" : i,
            "value" : value * (1 + .2 * random.random()),
            "ref_time" : time,
            "observation" : "<observation/heart_rate/{0}>".format(i),
            "observed_property": "<person1/heart_rate>",
            "sensor" : "<sensor2>",
        }
        tg = make_observation(data, False)
        out.write(re.sub(r"\s+", " ", tg))
        out.write("\n")

    out.close()


def make_activity_stream(prefixes, time, use_reification):
    file_name = "activity.trigs"
    if use_reification:
        file_name = "activity_reif.trigs"
    print("Generate stream: " + file_name)
    out = open(file_name, "w")
    out.write(re.sub(r"\s+", " ", prefixes))
    out.write("\n")

    value = "<activity/walking>"
    for i in range(60*10): # 10 min
        # Set actvity to resting after 30 sec
        if i == 30: value = "<activity/resting>"
 
        data = {
            "i" : i,
            "value" : value,
            "ref_time" : time,
            "observation" : "<observation/activity/{0}>".format(i),
            "observed_property": "<person1/activity>",
            "sensor" : "<sensor2>",
        }
        tg = make_observation(data, False)
        out.write(re.sub(r"\s+", " ", tg))
        out.write("\n")

    out.close()

def make_breathing_rate_stream(prefixes, time, use_reification):
    file_name = "breathing_rate.trigs"
    if use_reification:
        file_name = "breathing_rate.trigs"
    print("Generate stream: " + file_name)
    out = open(file_name, "w")
    out.write(re.sub(r"\s+", " ", prefixes))
    out.write("\n")

    value = breathing_rate_level["normal"]
    for i in range(60*10): # 10 min
        # Set breathing to elevated after 30 sec
        if i == 30: value = breathing_rate_level["elevated"]
 
        data = {
            "i" : i,
            "value" : value * (1 + .2 * random.random()),
            "ref_time" : time,
            "observation" : "<observation/breathing/{0}>".format(i),
            "observed_property": "<person1/breathing>",
            "sensor" : "<sensor3>",
        }
        tg = make_observation(data, False)
        out.write(re.sub(r"\s+", " ", tg))
        out.write("\n")

    out.close()

def main():
    prefixes = """
            @base <http://base/> .
            @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
            @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
            @prefix sosa: <http://www.w3.org/ns/sosa/> .
            @prefix prov: <http://www.w3.org/ns/prov#> .
            @prefix ex: <http://www.example.org/ontology#> ."""
        
    for use_reification in [False, True]:
        random.seed(0)
        ref_time = 1556617861
        make_heart_rate_stream(prefixes, ref_time, use_reification)
        make_activity_stream(prefixes, ref_time, use_reification)
        make_breathing_rate_stream(prefixes, ref_time, use_reification)

main()



