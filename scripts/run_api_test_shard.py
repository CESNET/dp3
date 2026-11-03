#!/usr/bin/env python3

import os
import sys
import time
import unittest


def main() -> int:
    sys.path.insert(0, os.path.abspath("tests/test_api"))

    loader = unittest.TestLoader()
    runner = unittest.TextTestRunner(verbosity=2)
    durations: list[tuple[str, float]] = []
    success = True

    for module_name in sys.argv[1:]:
        suite = loader.loadTestsFromName(module_name)
        start = time.monotonic()
        result = runner.run(suite)
        durations.append((module_name, time.monotonic() - start))
        success = success and result.wasSuccessful()

    print("\nModule durations:")
    for module_name, duration in sorted(durations, key=lambda d: d[1], reverse=True):
        print(f"  {duration:7.2f}s  {module_name}")
    print(f"  {sum(d for _, d in durations):7.2f}s  total")

    return 0 if success else 1


if __name__ == "__main__":
    raise SystemExit(main())
