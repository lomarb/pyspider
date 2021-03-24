import json
import unittest

from pyspider.result.convert import validate_response, convert


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
    "taskid": "abc",
}


class TestValidate(unittest.TestCase):

    def test_empty_response(self):
        self.assertRaises(ValueError, lambda: validate_response({}))

    def test_empty_body(self):
        self.assertRaises(ValueError, lambda: validate_response({"body": ""}))

    def test_empty_jurisdiction(self):
        j = dict(test_json)
        del j["jurisdiction"]
        self.assertRaises(ValueError, lambda: validate_response(j))

    def test_iso_code(self):
        j = dict(test_json)
        j["jurisdiction"] = "X"
        self.assertRaises(ValueError, lambda: validate_response(j))

    def test_source_type(self):
        j = dict(test_json)
        del j["source_type"]
        del j["document_type"]
        o = validate_response(j)
        self.assertEqual(o["source_type"], "UNDEFINED")
        self.assertEqual(o["document_type"], "UNKNOWN")

    def test_parse_date_fallback(self):
        o = validate_response({
            **test_json,
            "published": "in 2020/13/32",
        })
        self.assertEqual(type(o["published"]), float)

    def test_parse_date(self):
        o = validate_response({
            **test_json,
            "published": "hahaha 2011/03/11 and now something else",
        })
        self.assertEqual(1299801600.0, o["published"])


class TestConvert(unittest.TestCase):

    def test_convert(self):
        result = dict(test_json)
        task = dict(test_task)
        o = json.loads(convert(task, validate_response(result)))
        self.assertEqual(o["url"], task["url"])
        self.assertEqual(o["version"], o["crawl_time"])


if __name__ == "__main__":
    unittest.main()
