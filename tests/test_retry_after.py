from __future__ import annotations

import email.utils
import io
import unittest
import urllib.error
from email.message import Message

from app.http_utils import parse_retry_after_value, retry_after_from_urllib_http_error


class TestRetryAfterParsing(unittest.TestCase):
    def test_parse_delta_seconds(self) -> None:
        self.assertAlmostEqual(parse_retry_after_value("2.5"), 2.5, places=3)
        self.assertEqual(parse_retry_after_value("-1"), 0.0)

    def test_parse_http_date(self) -> None:
        now = 1_700_000_000.0
        header = email.utils.formatdate(now + 15.0, usegmt=True)
        out = parse_retry_after_value(header, now_ts=now)
        self.assertGreaterEqual(out, 14.0)
        self.assertLessEqual(out, 16.0)

    def test_parse_message_fallback(self) -> None:
        self.assertEqual(parse_retry_after_value("retry after 7 seconds"), 7.0)
        self.assertEqual(parse_retry_after_value("please wait 3 sec"), 3.0)
        self.assertEqual(parse_retry_after_value("n/a"), 0.0)

    def test_cap_applied(self) -> None:
        self.assertEqual(parse_retry_after_value("9999", max_wait_s=120.0), 120.0)

    def test_http_error_body_fallback(self) -> None:
        headers = Message()
        exc = urllib.error.HTTPError(
            url="https://example.test",
            code=429,
            msg="Too Many Requests",
            hdrs=headers,
            fp=io.BytesIO(b'{"message":"retry after 42 sec"}'),
        )
        self.assertEqual(retry_after_from_urllib_http_error(exc, max_wait_s=120.0), 42.0)


if __name__ == "__main__":
    unittest.main()
