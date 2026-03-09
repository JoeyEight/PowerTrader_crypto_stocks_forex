from __future__ import annotations

import unittest

from app.api_endpoint_validation import (
    ALPACA_LIVE_HOST,
    ALPACA_PAPER_HOST,
    OANDA_LIVE_REST_HOST,
    OANDA_PRACTICE_REST_HOST,
    normalize_endpoint_url,
    validate_alpaca_endpoints,
    validate_oanda_endpoints,
)


class TestApiEndpointValidation(unittest.TestCase):
    def test_normalize_endpoint_adds_https(self) -> None:
        normalized, ok, host = normalize_endpoint_url("paper-api.alpaca.markets")
        self.assertTrue(ok)
        self.assertEqual(normalized, f"https://{ALPACA_PAPER_HOST}")
        self.assertEqual(host, ALPACA_PAPER_HOST)

    def test_validate_alpaca_mode_mismatch(self) -> None:
        out = validate_alpaca_endpoints(
            base_url=f"https://{ALPACA_LIVE_HOST}",
            data_url="https://data.alpaca.markets",
            paper_mode=True,
        )
        codes = {str(row.get("code", "")) for row in list(out.get("issues", []) or []) if isinstance(row, dict)}
        self.assertIn("alpaca_paper_mode_uses_live_endpoint", codes)

    def test_validate_oanda_mode_mismatch(self) -> None:
        out = validate_oanda_endpoints(
            rest_url=f"https://{OANDA_LIVE_REST_HOST}",
            stream_url="https://stream-fxpractice.oanda.com",
            practice_mode=True,
        )
        codes = {str(row.get("code", "")) for row in list(out.get("issues", []) or []) if isinstance(row, dict)}
        self.assertIn("oanda_practice_mode_uses_live_endpoint", codes)

    def test_validate_oanda_invalid_url_is_critical(self) -> None:
        out = validate_oanda_endpoints(rest_url="://bad-url", stream_url="://bad-stream", practice_mode=True)
        issues = [row for row in list(out.get("issues", []) or []) if isinstance(row, dict)]
        critical_codes = {str(row.get("code", "")) for row in issues if str(row.get("level", "")).lower() == "critical"}
        self.assertIn("oanda_rest_url_invalid", critical_codes)
        self.assertIn("oanda_stream_url_invalid", critical_codes)

    def test_oanda_defaults_follow_mode(self) -> None:
        practice = validate_oanda_endpoints(rest_url="", stream_url="", practice_mode=True)
        live = validate_oanda_endpoints(rest_url="", stream_url="", practice_mode=False)
        self.assertIn(OANDA_PRACTICE_REST_HOST, str(practice.get("normalized_rest_url", "")))
        self.assertIn(OANDA_LIVE_REST_HOST, str(live.get("normalized_rest_url", "")))


if __name__ == "__main__":
    unittest.main()

