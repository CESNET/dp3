# Very simple datapoint generator for bus example config

import random
from argparse import ArgumentParser
from datetime import datetime, timedelta
from sys import stderr
from time import sleep

import requests


def random_initial_location():
    latitude = random.uniform(39.0, 41.0)
    longitute = random.uniform(18.0, 22.0)

    return [round(latitude, 3), round(longitute, 3)]


def do_random_location_increment(current_location):
    current_location[0] = round(current_location[0] + random.uniform(-1.0, 1.0), 3)
    current_location[1] = round(current_location[1] + random.uniform(-1.0, 1.0), 3)


def t1():
    return datetime.utcnow()


def random_t2():
    return datetime.utcnow() + timedelta(minutes=random.randint(5, 15))


def random_passenger_counts_3():
    return [random.randint(0, 40), random.randint(0, 40), random.randint(0, 40)]


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


if __name__ == "__main__":
    # Parse arguments
    parser = ArgumentParser("Simple datapoint generator for DP3 testing.")
    parser.add_argument(
        "--endpoint-url",
        help="Base DP3 API endpoint. Default='http://127.0.0.1:5000'.",
        default="http://127.0.0.1:5000",
        type=str,
    )
    parser.add_argument(
        "--interval",
        help="Interval of sending datapoints in minutes. Default: 1 minute.",
        default=1,
        type=int,
    )
    args = parser.parse_args()

    # API endpoint
    datapoints_url = f"{args.endpoint_url}/datapoints"

    # Datapoints buffer
    dps = []

    # One-time datapoints
    for n, bus_line in bus_lines.items():
        dps.append(
            {
                "type": "bus",
                "id": n,
                "attr": "label",
                "v": bus_line["label"],
                "t1": t1().isoformat(),
                "t2": random_t2().isoformat(),
                "src": "Manual",
            }
        )

    # Realtime datapoints
    while True:
        for n, bus_line in bus_lines.items():
            dps.append(
                {
                    "type": "bus",
                    "id": n,
                    "attr": "location",
                    "v": bus_line["location"],
                    "t1": t1().isoformat(),
                    "t2": random_t2().isoformat(),
                    "src": "Tracking SW",
                }
            )
            do_random_location_increment(bus_line["location"])

            dps.append(
                {
                    "type": "bus",
                    "id": n,
                    "attr": "speed",
                    "v": round(random.uniform(0.0, 100.0), 1),
                    "t1": t1().isoformat(),
                    "t2": random_t2().isoformat(),
                    "src": "Tracking SW",
                }
            )

            t1_local = t1()
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
                    "t1": t1_local.isoformat(),
                    "t2": (t1_local + timedelta(minutes=30)).isoformat(),
                    "src": "Bus counter",
                }
            )

        # Send datapoints
        try:
            response = requests.post(url=datapoints_url, json=dps)

            if response.status_code == requests.codes.ok:
                print(f"Sent {len(dps)} datapoints of attribute(s)")
            else:
                response_content = response.content.decode("utf-8")
                print(
                    f"Sending datapoints failed: {response.reason} {response_content}",
                    file=stderr,
                )
                print(f"Payload: {dps}", file=stderr)

        except requests.exceptions.ConnectionError:
            print("Sending datapoints failed: connection error", file=stderr)

        except KeyboardInterrupt:
            print("Received keyboard interrupt, exiting.", file=stderr)
            break

        # Clear buffer
        dps = []

        # Wait for next cycle
        sleep(60 * args.interval)
