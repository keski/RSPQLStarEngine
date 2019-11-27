"""
Author: Robin Keskisärkkä
Description:
This script can be used to generate uncertain data streams. The generated streams are aligned with respect to time, and
change with respect to "triggers" on a timeline. Each trigger results in a change in the values for the generated
events, allowing specific scenarios to be used to as guide for the generated streams.

Each streamed is written to a separate file, with each streamed element written to a single line in trigs syntax.

The uncertainty and stream rate associated with each stream can configured by modifying the properties.conf.

"""


import re
from datetime import datetime
import time as t
import numpy as np
import matplotlib.pyplot as plt
import skfuzzy as fuzz

from mako.template import Template


np.random.seed(0)

# Sensors:
# - activity: reported by system, no uncertainty
# - heart rate: 1 sensor on body
# - breathing rate: 1 sensor on body
# - temperature: 1 sensor on body
# - oxygen saturation: 1 sensor on body


def main_():
    # Generate universe variables
    #   * Quality and service on subjective ranges [0, 10]
    #   * Tip has a range of [0, 25] in units of percentage points
    hr_state = []
    x_hr = np.arange(0, 201, 1)

    # Generate fuzzy membership functions
    hr_lo = fuzz.trapmf(x_hr, [0, 0, 60, 80])
    hr_md = fuzz.trapmf(x_hr, [60, 80, 100, 120])
    hr_hi = fuzz.trapmf(x_hr, [100, 120, 160, 180])
    hr_vhi = fuzz.trapmf(x_hr, [160, 180, 200, 200])

    # Visualize these universes and membership functions
    fig, ax = plt.subplots(nrows=1, figsize=(8, 9))

    ax.plot(x_hr, hr_lo, 'b', linewidth=1.5, label='Low')
    ax.plot(x_hr, hr_md, 'g', linewidth=1.5, label='Medium')
    ax.plot(x_hr, hr_hi, 'r', linewidth=1.5, label='High')
    ax.plot(x_hr, hr_vhi, 'y', linewidth=1.5, label='VeryHigh')
    ax.set_title('Heart rate')
    ax.legend()

    # Turn off top/right axes
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()

    plt.tight_layout()
    plt.show()




def main():
    normal_sample.__name__ = "Normal"
    uniform_sample.__name__ = "Uniform"

    heart_f = open("../src/main/resources/use-case/streams/heart.trigs", "w")
    oxygen_f = open("../src/main/resources/use-case/streams/oxygen.trigs", "w")
    breathing_f = open("../src/main/resources/use-case/streams/breathing.trigs", "w")
    temperature_f = open("../src/main/resources/use-case/streams/temperature.trigs", "w")
    activity_f = open("../src/main/resources/use-case/streams/activity.trigs", "w")

    # write prefixes
    with open("prefixes.ttl", "r") as f:
        prefixes = f.read().replace("\n", " ")
    heart_f.write(prefixes + "\n")

    scenario = [
        {
            "duration": 100,
            "heart_rate": 60,
            "oxygen": 95,
            "breathing": 14,
            "temperature": 37,
            "activity": ":Resting"
        },
        {
            "duration": 100,
            "heart_rate": 80,
            "oxygen": 95,
            "breathing": 14,
            "temperature": 37,
            "activity": ":Resting"
        },
        {
            "duration": 100,
            "heart_rate": 100,
            "oxygen": 94,
            "breathing": 14,
            "temperature": 38,
            "activity": ":Sleeping"
        },
        {
            "duration": 100,
            "heart_rate": 120,
            "oxygen": 90,
            "breathing": 20,
            "temperature": 38.5,
            "activity": ":Sleeping"
        },
        {
            "duration": 100,
            "heart_rate": 130,
            "oxygen": 89,
            "breathing": 14,
            "temperature": 38.8,
            "activity": ":Sleeping"
        },{
            "duration": 100,
            "heart_rate": 130,
            "oxygen": 89,
            "breathing": 25,
            "temperature": 38.8,
            "activity": ":Sleeping"
        },
    ]

    unix_time = t.time()
    counter = 0
    for x in scenario:
        for i in range(x["duration"]):
            counter += 1
            unix_time += 1
            timestamp = f"\"{datetime.utcfromtimestamp(unix_time).strftime('%Y-%m-%dT%H:%M:%S')}\"^^xsd:dateTime"

            event = heart(counter, timestamp, x["heart_rate"], 5)
            heart_f.write(re.sub("\\s+", " ", event) + "\n")

            event = oxygen(counter, timestamp, x["oxygen"], 2)
            oxygen_f.write(re.sub("\\s+", " ", event) + "\n")

            event = breathing(counter, timestamp, x["breathing"], 2)
            breathing_f.write(re.sub("\\s+", " ", event) + "\n")

            event = temperature(counter, timestamp, x["temperature"], 0.5)
            temperature_f.write(re.sub("\\s+", " ", event) + "\n")

            event = activity(counter, timestamp, x["activity"])
            activity_f.write(re.sub("\\s+", " ", event) + "\n")

    heart_f.close()
    oxygen_f.close()


def heart(i, timestamp, mean, stddev, person="person1", sensor="hr/sensor1"):
    sample = int(normal_sample(mean, stddev))
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/HeartRate>",
        "value": sample,
        "value_error": f"\"Normal(0,{stddev})\"^^rspu:distribution",
        "unc_type": get_fuzzy_heart_event_type(sample),
        "time": timestamp,
        "state_type": ":HeartRate",
        "state": get_hr_state(sample)
    }
    template = Template(filename='heart.template')
    return template.render(data=data)


def oxygen(i, timestamp, mean, error, person="person1", sensor="oxygen/sensor1"):
    sample = int(uniform_sample(mean-error, mean+error))
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/OxygenSaturation>",
        "value": sample,
        "value_error": f"\"Uniform({-error},{error})\"^^rspu:distribution",
        "unc_type": get_fuzzy_oxygen_event_type(sample),
        "time": timestamp,
        "state_type": ":OxygenSaturation",
        "state": get_oxygen_state(sample)
    }
    template = Template(filename='oxygen.template')
    return template.render(data=data)


def breathing(i, timestamp, mean, error, person="person1", sensor="breathing/sensor1"):
    sample = int(uniform_sample(mean-error, mean+error))
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/BreathingRate>",
        "value": sample,
        "value_error": f"\"Uniform({-error},{error})\"^^rspu:distribution",
        "unc_type": get_fuzzy_breathing_event_type(sample),
        "time": timestamp,
        "state_type": ":BreathingRate",
        "state": get_breathing_state(sample)
    }
    template = Template(filename='breathing.template')
    return template.render(data=data)


def temperature(i, timestamp, mean, stddev, person="person1", sensor="temperature/sensor1"):
    sample = int(normal_sample(mean, stddev))
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/Temperature>",
        "value": sample,
        "value_error": f"\"Normal(0,0.5)\"^^rspu:distribution",
        "unc_type": get_fuzzy_temperature_event_type(sample),
        "time": timestamp,
        "state_type": ":Temperature",
        "state": get_temperature_state(sample)
    }
    template = Template(filename='temperature.template')
    return template.render(data=data)


def activity(i, timestamp, doing, person="person1", sensor="activity/sensor1"):
    data = {
        "graph": f"_:g{i}",
        "observation": f"_:b{i}",
        "sensor": f"<{sensor}>",
        "feature_of_interest": f"<{person}>",
        "observed_property": f"<{person}/Temperature>",
        "value": doing,
        "time": timestamp,
        "state_type": ":Activity",
        "state": doing
    }
    template = Template(filename='activity.template')
    return template.render(data=data)


def get_hr_state(value):
    """
    Returns the most probable state of the Heart Rate node, given a numeric value.
    :param value: mean of heart rate
    :return:
    """
    x_hr = np.arange(0, 201, 1)
    # Fuzzy membership functions
    hr_lo = fuzz.trapmf(x_hr, [0, 0, 60, 80])
    hr_md = fuzz.trapmf(x_hr, [60, 80, 100, 120])
    hr_hi = fuzz.trapmf(x_hr, [100, 120, 160, 180])
    hr_vhi = fuzz.trapmf(x_hr, [160, 180, 200, 200])

    event_types = [":Low", ":Normal", ":High", ":VeryHigh"]
    probs = [hr_lo[value], hr_md[value], hr_hi[value], hr_vhi[value]]
    return event_types[np.argmax(probs)]


def get_fuzzy_heart_event_type(value):
    """
    Returns the fuzzy event types of heart rate observations.
    :param value: mean of heart rate
    :return:
    """
    x_hr = np.arange(0, 201, 1)
    # Fuzzy membership functions
    hr_lo = fuzz.trapmf(x_hr, [0, 0, 60, 80])
    hr_md = fuzz.trapmf(x_hr, [60, 80, 100, 120])
    hr_hi = fuzz.trapmf(x_hr, [100, 120, 160, 180])
    hr_vhi = fuzz.trapmf(x_hr, [160, 180, 200, 200])

    return {":LowHeartRateEvent": hr_lo[value],
            ":NormalHeartRateEvent": hr_md[value],
            ":HighHeartRateEvent": hr_hi[value],
            ":VeryHighHeartRateEvent": hr_vhi[value]}


def get_oxygen_state(value):
    """
    Returns the most probable state of the oxygen saturation node, given a numeric value.
    :param value: mean of oxygen saturation
    :return:
    """
    x = np.arange(0, 100, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 0, 88, 90])
    normal = fuzz.trapmf(x, [88, 92, 100, 100])

    states = [":Low", ":Normal"]
    probs = [low[value], normal[value]]
    return states[np.argmax(probs)]


def get_fuzzy_oxygen_event_type(value):
    """
    Returns the fuzzy event types of heart rate observations.
    :param value: mean of heart rate
    :return:
    """
    x = np.arange(0, 100, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 0, 88, 90])
    normal = fuzz.trapmf(x, [88, 92, 100, 100])
    return {":LowOxygenSaturationEvent": low[value], ":NormalOxygenSaturationEvent": normal[value]}


def get_breathing_state(value):
    """
    Returns the most probable state of the oxygen saturation node, given a numeric value.
    :param value: mean of oxygen saturation
    :return:
    """
    x = np.arange(0, 60, 1)
    # Fuzzy membership functions
    low = fuzz.trimf(x, [0, 7, 9])
    normal = fuzz.trapmf(x, [8, 12, 16, 20])
    high = fuzz.trapmf(x, [18, 60, 100, 100])

    states = [":Low", ":Normal", ":High"]
    probs = [low[value], normal[value], high[value]]
    return states[np.argmax(probs)]


def get_fuzzy_breathing_event_type(value):
    """
    Returns the fuzzy event types of heart rate observations.
    :param value: mean of heart rate
    :return:
    """
    x = np.arange(0, 60, 1)
    # Fuzzy membership functions
    low = fuzz.trimf(x, [0, 7, 9])
    normal = fuzz.trapmf(x, [8, 12, 16, 20])
    high = fuzz.trapmf(x, [18, 60, 100, 100])
    return { ":SlowBreathingRateEvent": low[value],
             ":NormalBreathingRateEvent": normal[value],
             ":ElevatedBreathingRateEvent": high[value]}


def get_temperature_state(value):
    """
    Returns the most probable state of the temperature saturation node, given a numeric value.
    :param value: mean of temperature
    :return:
    """
    x = np.arange(0, 100, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 30, 35, 36])
    normal = fuzz.trapmf(x, [35, 36, 37.5, 38])
    high = fuzz.trapmf(x, [37.5, 38, 42, 100])

    states = [":Low", ":Normal", ":High"]
    probs = [low[value], normal[value], high[value]]
    return states[np.argmax(probs)]


def get_fuzzy_temperature_event_type(value):
    """
    Returns the fuzzy event types of temperature observations.
    :param value: mean of temperature
    :return:
    """
    x = np.arange(0, 100, 1)
    # Fuzzy membership functions
    low = fuzz.trapmf(x, [0, 30, 35, 36])
    normal = fuzz.trapmf(x, [35, 36, 37.5, 38])
    high = fuzz.trapmf(x, [37.5, 38, 42, 100])
    return { ":SlowBreathingRateEvent": low[value],
             ":NormalBreathingRateEvent": normal[value],
             ":ElevatedBreathingRateEvent": high[value]}


def normal_sample(mu, sigma, decimals=2):
    return np.around(np.random.normal(mu, sigma), decimals)


def uniform_sample(lower, upper, decimals=2):
    return np.around(np.random.uniform(lower, upper), decimals)


if __name__ == '__main__':
    main()

