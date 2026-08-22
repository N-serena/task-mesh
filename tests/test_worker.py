from __future__ import annotations

import unittest

from engine.classifier import Route, classify_exception


class ClassifierTests(unittest.TestCase):
    def test_validation_error_routes_to_dlq(self) -> None:
        self.assertEqual(classify_exception(ValueError("bad schema")), Route.ROUTE_TO_DLQ)


if __name__ == "__main__":
    unittest.main()

