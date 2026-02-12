from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Dict, List, Any

from src.utils.config import load_settings
from src.brokers.kis_broker import KISBroker


class KISPriceClient:
    """간단한 KIS 기간별시세(일봉) 클라이언트."""

    def __init__(self, settings: Dict[str, Any] | None = None):
        self.settings = settings or load_settings()
        self.kis = self.settings["kis"]
        self.broker = KISBroker(self.settings)
        self.base_url = self.kis.get(
            "base_url_prod" if self.settings.get("env", "paper") == "prod" else "base_url_paper"
        )
        # self.rate_sleep is no longer needed; KISBroker handles it.

    def _tr_id(self) -> str:
        # 국내주식 기간별 시세(일/주/월/년) TR
        return "FHKST03010100"

    def _tr_id_overseas_daily(self) -> str:
        # 해외주식 기간별시세 TR
        return "HHDFS76240000"

    def _tr_id_overseas_info(self) -> str:
        # 해외주식 상품기본정보 TR
        return "CTPF1702R"

    def get_daily_prices(self, code: str, start: str, end: str) -> Dict[str, Any]:
        """start/end: YYYYMMDD"""
        tr_id = self._tr_id()
        url = f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD": code,
            "FID_INPUT_DATE_1": start,
            "FID_INPUT_DATE_2": end,
            "FID_PERIOD_DIV_CODE": "D",
            "FID_ORG_ADJ_PRC": "1",
        }
        return self.broker.request(tr_id, url, params=params)

    def get_overseas_daily_prices(self, excd: str, symbol: str, bymd: str, gubn: str = "0", modp: str = "1", keyb: str = "") -> Dict[str, Any]:
        """해외주식 기간별시세 (일봉). bymd: YYYYMMDD"""
        tr_id = self._tr_id_overseas_daily()
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/dailyprice"
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": symbol,
            "GUBN": gubn,
            "BYMD": bymd,
            "MODP": modp,
        }
        if keyb:
            params["KEYB"] = keyb
        return self.broker.request(tr_id, url, params=params)

    def get_overseas_stock_info(self, prdt_type_cd: str, symbol: str) -> Dict[str, Any]:
        """해외주식 상품기본정보."""
        tr_id = self._tr_id_overseas_info()
        url = f"{self.base_url}/uapi/overseas-price/v1/quotations/search-info"
        params = {
            "PRDT_TYPE_CD": prdt_type_cd,
            "PDNO": symbol,
        }
        return self.broker.request(tr_id, url, params=params)
