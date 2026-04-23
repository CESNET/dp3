#!/usr/bin/env python3

import os
import sys
import unittest


def main() -> int:
    sys.path.insert(0, os.path.abspath("tests/test_api"))

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for module_name in sys.argv[1:]:
        suite.addTests(loader.loadTestsFromName(module_name))

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
