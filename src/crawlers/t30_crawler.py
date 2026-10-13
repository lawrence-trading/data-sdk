"""T30 漲跌幅度資料檔 crawler.

Downloads T30V.TSE / T30V.OTC (the exchanges' daily T30 files as relayed by the
broker host) and parses the fixed-width records into T30Record.

    from data_sdk.crawlers import T30Crawler
    records = T30Crawler().fetch_all()

Layout: 證交所「一般交易電腦作業手冊」V6.2 and 櫃買「上櫃股票-等價交易電腦作業手冊」
(2026-09). Bytes 1-85 are identical on both markets; 櫃買 inserts 類股註記 at
byte 86, which shifts its last three fields by one.
"""
from dataclasses import dataclass
from decimal import Decimal

import requests

BASE_URL = "http://210.59.202.15/download/"
RECORD_LEN = 100


@dataclass
class T30Record:
    code: str
    market: str           # "TSE" | "OTC"
    kind: str             # "stock" | "etf" | "warrant" | "other" (derived, see classify)
    limit_up: Decimal           # BULL-PRICE 漲停價
    reference_price: Decimal    # LDC-PRICE 開盤競價基準
    limit_down: Decimal         # BEAR-PRICE 跌停價
    last_mth_date: str    # LAST-MTH-DATE 上次成交日 YYYYMMDD
    settype: str          # 交易方式: "0" 一般, "1" 變更交易, "2" 變更交易且分盤   (註1)
    mark_w: str           # 處置股票註記: "0" 正常, "1" 處置, "2" 再次處置, "3" 彈性處置 (註2)
    mark_p: str           # 注意股票註記: "0" / "1"                               (註3)
    mark_l: str           # 委託限制註記: "0" / "1" 證券商委託申報受限              (註4)
    ind_code: str         # 產業別代碼 (表一); 權證等無產業屬性者為 "00"             (註5)
    sec_type: str         # 證券別代碼 (表三): W1-W4 權證, BS/FB/RR/RS/RB, 或空白   (註6)
    mark_m: str           # 豁免平盤下融券賣出註記: "0" / "1"                      (註7)
    name: str             # 股票中文名稱
    match_interval: int   # 撮合循環時間 (分), 分盤或處置時有值                     (註8)
    order_limit: int      # 單筆委託達此數量須預收款券 (交易單位), 0 = 無
    orders_limit: int     # 多筆委託累積達此數量須預收款券 (交易單位), 0 = 無
    prepay_rate: int      # 款券預收成數 (%)
    mark_s: str           # 豁免平盤下借券賣出註記: "0" / "1"                      (註9)
    stk_mark: str         # 類股註記, 櫃買限定: "0" 一般, "1" 中小企業股票; 上市為 ""
    mark_f: str           # 面額註記: "0" 正常, "1" 面額非 10 元
    day_trade: str        # 可現股當沖: "Y" 先買後賣, "X" 雙向, " " 不可
    board: str            # 板別註記: "0" 一般, "3" 創新板 (上市); 櫃買預設 "0"


class T30Crawler:
    def __init__(self, base_url: str = BASE_URL, timeout: int = 30):
        self.base_url = base_url
        self.timeout = timeout

    def fetch(self, market: str) -> list[T30Record]:
        url = f"{self.base_url}T30V.{market}"
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()

        return self.parse(response.content, market)

    def fetch_all(self) -> list[T30Record]:
        return self.fetch("TSE") + self.fetch("OTC")

    def parse(self, content: bytes, market: str) -> list[T30Record]:
        # 櫃買官網的 t30.dat 每筆後面有換行, T30V 沒有.
        content = content.replace(b"\r", b"").replace(b"\n", b"")

        if len(content) % RECORD_LEN:
            raise ValueError(
                f"{market}: {len(content)} bytes is not a multiple of {RECORD_LEN}"
            )

        records = []
        for offset in range(0, len(content), RECORD_LEN):
            record_bytes = content[offset:offset + RECORD_LEN]
            records.append(self.parse_record(record_bytes, market))
        return records

    def parse_record(self, record: bytes, market: str) -> T30Record:
        # Positions below are the manual's: 1-based start, then length.
        def field(start: int, length: int) -> bytes:
            return record[start - 1:start - 1 + length]

        def text(start: int, length: int) -> str:
            return field(start, length).decode("ascii")

        def number(start: int, length: int) -> int:
            return int(field(start, length))

        def price(start: int) -> Decimal:
            return Decimal(text(start, 9)).scaleb(-4)    # 9(5)V9(4)

        # 櫃買在第 86 碼多一個類股註記, 之後的面額 / 當沖 / 板別各後移一碼.
        is_otc = market == "OTC"
        tail_start = 87 if is_otc else 86

        code = text(1, 6).strip()
        sec_type = text(48, 2)

        return T30Record(
            # 識別
            code=code,
            market=market,
            kind=self.classify(code, sec_type),
            limit_up=price(7),
            reference_price=price(16),
            limit_down=price(25),
            last_mth_date=text(34, 8),
            settype=text(42, 1),
            mark_w=text(43, 1),
            mark_p=text(44, 1),
            mark_l=text(45, 1),
            ind_code=text(46, 2),
            sec_type=sec_type,
            mark_m=text(50, 1),
            name=field(51, 16).decode("cp950", "replace").strip(),
            match_interval=number(67, 3),
            order_limit=number(70, 6),
            orders_limit=number(76, 6),
            prepay_rate=number(82, 3),
            mark_s=text(85, 1),
            stk_mark=text(86, 1) if is_otc else "",
            mark_f=text(tail_start, 1),
            day_trade=text(tail_start + 1, 1),
            board=text(tail_start + 2, 1),
        )

    @staticmethod
    def classify(code: str, sec_type: str) -> str:
        if sec_type.startswith("W"):    # 證券別代碼 W1-W4 (手冊表三)
            return "warrant"

        if code.startswith("00"):
            return "etf"

        if len(code) == 4 and code.isdigit():
            return "stock"

        return "other"     # 特別股, TDR, ETN, 受益憑證, 可轉債...

