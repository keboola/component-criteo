"""
Created on 12. 11. 2018

@author: esner
"""

import os
import unittest
from unittest import mock

from freezegun import freeze_time

from component import Component
from criteo import CriteoClientException


class TestComponent(unittest.TestCase):
    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()


class TestParseError(unittest.TestCase):
    """Regression tests for Component.parse_error.

    The Criteo API error body is expected to decode (via json.loads) to a dict with
    "errors"/"error" fields. If the API instead returns a body that decodes to a
    non-dict value (e.g. a plain JSON string), parse_error must still return a
    string description instead of crashing with an unhandled AttributeError
    (the crash previously escalated a recoverable API error into an opaque
    internal error / exit code 2 instead of a UserException).
    """

    @staticmethod
    def _make_exception(body):
        api_exc = mock.Mock()
        api_exc.body = body
        return CriteoClientException(api_exc)

    def test_dict_error_with_errors_list_still_parsed(self):
        # Existing/happy-path behaviour must be unchanged for a structured dict body.
        exc = self._make_exception(b'{"errors": [{"code": "BAD_REQUEST", "detail": "bad dates"}]}')
        result = Component.parse_error(exc)
        self.assertIn("BAD_REQUEST", result)
        self.assertIn("bad dates", result)

    def test_dict_error_without_errors_key_still_parsed(self):
        # Existing/happy-path behaviour must be unchanged for a single-level error dict.
        exc = self._make_exception(b'{"error": "invalid_grant", "error_description": "bad creds"}')
        result = Component.parse_error(exc)
        self.assertIn("invalid_grant", result)
        self.assertIn("bad creds", result)

    def test_non_dict_json_body_does_not_raise(self):
        # A JSON body that decodes to a plain string (not a dict) previously crashed
        # with "AttributeError: 'str' object has no attribute 'get'" instead of
        # producing a UserException with a readable message.
        exc = self._make_exception(b'"Rate limit exceeded"')
        result = Component.parse_error(exc)
        self.assertEqual(result, "Rate limit exceeded")

    def test_non_dict_json_list_body_does_not_raise(self):
        exc = self._make_exception(b'["unexpected", "list", "body"]')
        result = Component.parse_error(exc)
        self.assertEqual(result, str(["unexpected", "list", "body"]))


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
