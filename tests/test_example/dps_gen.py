# Very simple datapoint generator for bus example config

import datetime
import json
import random


class TimeContainer:
    def __init__(self):
        self.time = datetime.datetime.utcnow() - datetime.timedelta(days=4)

    def add_minutes(self, minutes: int):
        self.time += datetime.timedelta(minutes=minutes)
        return self.time

    def add_minutes_no_modify(self, minutes: int):
        return self.time + datetime.timedelta(minutes=minutes)


time = TimeContainer()


def random_initial_location():
    latitude = random.uniform(39.0, 41.0)
    longitute = random.uniform(18.0, 22.0)

    return [round(latitude, 3), round(longitute, 3)]


def do_random_location_increment(current_location):
    current_location[0] = round(current_location[0] + random.uniform(-1.0, 1.0), 3)
    current_location[1] = round(current_location[1] + random.uniform(-1.0, 1.0), 3)


def random_t1(pseudo_current_time):
    return pseudo_current_time.add_minutes(random.randint(0, 60))


def random_t2(pseudo_current_time):
    return pseudo_current_time.add_minutes_no_modify(random.randint(5, 15))


def random_passenger_counts_3():
    return [random.randint(0, 40), random.randint(0, 40), random.randint(0, 40)]


dps = []

bus_lines = {
    "10": {"label": "Special bus 10", "location": random_initial_location()},
    "11": {"label": "Special bus 11", "location": random_initial_location()},
    "12": {"label": "Bus 12", "location": random_initial_location()},
    "19": {"label": "Bus 19 ", "location": random_initial_location()},
    "48": {"label": "Bus 48", "location": random_initial_location()},
    "93": {"label": "Bus 93", "location": random_initial_location()},
    "E73": {"label": "Express bus 73", "location": random_initial_location()},
    "N72": {"label": "Night bus 72", "location": random_initial_location()},
    "S20": {"label": "School bus 20", "location": random_initial_location()},
}

for n, bus_line in bus_lines.items():
    dps.append(
        {
            "type": "bus",
            "id": n,
            "attr": "label",
            "v": bus_line["label"],
            "t1": random_t1(
                time,
            ).isoformat(),
            "t2": random_t2(
                time,
            ).isoformat(),
            "src": "Manual",
        }
    )

    for _i in range(5):
        dps.append(
            {
                "type": "bus",
                "id": n,
                "attr": "location",
                "v": bus_line["location"],
                "t1": random_t1(
                    time,
                ).isoformat(),
                "t2": random_t2(
                    time,
                ).isoformat(),
                "src": "Tracking SW",
            }
        )
        do_random_location_increment(bus_line["location"])

    for _i in range(5):
        dps.append(
            {
                "type": "bus",
                "id": n,
                "attr": "speed",
                "v": round(random.uniform(0.0, 100.0), 1),
                "t1": random_t1(
                    time,
                ).isoformat(),
                "t2": random_t2(
                    time,
                ).isoformat(),
                "src": "Tracking SW",
            }
        )

    for _i in range(5):
        random_t1_local = random_t1(
            time,
        )

        dps.append(
            {
                "type": "bus",
                "id": n,
                "attr": "passengers_in_out",
                "v": {
                    "front_in": random_passenger_counts_3(),
                    "front_out": random_passenger_counts_3(),
                    "middle_in": random_passenger_counts_3(),
                    "middle_out": random_passenger_counts_3(),
                    "back_in": random_passenger_counts_3(),
                    "back_out": random_passenger_counts_3(),
                },
                "t1": random_t1_local.isoformat(),
                "t2": (random_t1_local + datetime.timedelta(minutes=30)).isoformat(),
                "src": "Bus counter",
            }
        )

print(json.dumps(dps, indent=2))
