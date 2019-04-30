#!/bin/python3

import random
import re
from datetime import datetime
# Note that all annotations supported by SSN require the original value to be wrapped
# as an instance. I.e. statement-level meta data is not supported.


# detection stream: located in room, show movement or not

"""
# ontology stuff
<person1>
    a foaf:Person ;
    foaf:age 71 ;
    rdfs:comment "This resource represents Rut in the paper scenario" .

<person1/heartRate>
    a sosa:ObservableProperty ;
    sosa:isObservedBy <sensor1> ;
    ssn:isPropertyOf <person1> ;
    rdfs:label "Measure of a Rut's heart rate"@en .

<sensor1>
    a sosa:Sensor ;
    sosa:observes <person1/heartRate> .

# sensor stuff
<Observation/346344> a sosa:Observation ;
    sosa:observedProperty <person1/heartRate> ;
    sosa:hasFeatureOfInterest <person1> ;
    sosa:madeBySensor <sensor1> ;
    sosa:hasSimpleResult "70.0"^^xsd:float ;
    sosa:resultTime "2019-05-06T12:36:12Z"^^xsd:dateTime .

"""


activity_map = {
    "resting" : {
        "breathing" : "normal",
        "heart rate" : 70,
        "heart rate elevated" : 120,
        "motion rate" : "low"
    }
}

def heart_rate(i, heart_rate_avg, t):
    value = (1 + .2 * random.random()) * heart_rate_avg
    time = datetime.utcfromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')
    test = """
_:{0} {{
    <observation/{0}> a sosa:Observation ;
        sosa:observedProperty <person1/heart_rate> ;
        sosa:hasFeatureOfInterest <person1> ;
        sosa:madeBySensor <sensor1> ;
        sosa:resultTime "{1}"^^xsd:dateTime .
    <<<observation/{0}> sosa:hasSimpleResult {2}>>  ex:confidence {3} .
}} .
<observation/{0}> prov:generatedAtTime "{1}"^^xsd:dateTime .""".format(i, time, value, 0.9)
    return test

out = open("heart_rate.trigs", "w")
prefixes = """@base <http://base/> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix sosa: <http://www.w3.org/ns/sosa/> .
@prefix ssn: <http://www.w3.org/ns/ssn/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix ex: <http:///example.org/ontology#> .
"""
out.write(re.sub(r"\s+", " ", prefixes))
out.write("\n")

heart_rate_avg = 71
ref_time = 1556617861
for i in range(10000): # 1 hour
    ref_time += 1
    if i == 60: heart_rate_avg = 120
    observation = heart_rate(i, heart_rate_avg, ref_time)
    print(observation)
    out.write(re.sub(r"\s+", " ", observation))
    out.write("\n")
out.close()










