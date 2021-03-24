import json
import unittest

from pyspider.result.convert import validate, convert


test_json = {
    "published": 1230923.0,
    "title": "TITLE",
    "body": "BODY",
    "institution_name": "IN",
    "short_institution_name": "SIN",
    "source_type": "ST",
    "document_type": "DT",
    "expected_language_code": "en",
    "jurisdiction": "US",
    "importance": "LOW",
}

test_task = {
    "url": "url",
    "timestamp": 123.4,
}


class TestValidate(unittest.TestCase):

    def test_empty_response(self):
        self.assertRaises(ValueError, validate(lambda: {}))

    def test_empty_body(self):
        self.assertRaises(ValueError, validate(lambda: {"body": ""}))

    def test_empty_jurisdiction(self):
        j = dict(test_json)
        del j["jurisdiction"]
        self.assertRaises(ValueError, validate(lambda: j))

    def test_iso_code(self):
        j = dict(test_json)
        j["jurisdiction"] = "X"
        self.assertRaises(ValueError, validate(lambda: j))

    def test_source_type(self):
        j = dict(test_json)
        del j["source_type"]
        o = validate(lambda: j)()
        self.assertEqual(o["source_type"], "UNDEFINED")


class TestConvert(unittest.TestCase):

    def test_convert(self):
        result = dict(test_json)
        task = dict(test_task)
        o = json.loads(convert(task, validate(lambda: result)()))
        self.assertEqual(o["url"], task["url"])
        self.assertEqual(o["version"], o["crawl_time"])


if __name__ == "__main__":
    unittest.main()
