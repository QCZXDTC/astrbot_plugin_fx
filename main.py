import asyncio
import csv
import io
import json
import math
import os
import re
import tempfile
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any
import aiohttp
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, StarTools
try:
    from astrbot.api.provider import ProviderRequest
except ImportError:
    ProviderRequest = Any

PAIRS = ("EURUSD", "GBPUSD", "USDJPY", "XAUUSD")
PRICE_DIGITS = {"EURUSD": 5, "GBPUSD": 5, "USDJPY": 3, "XAUUSD": 2}
TOKEN_PER_USD = 10000          # 10000 token 合 1 美元
NEW_USER_BONUS = 500000        # 新手奖励
QUOTE_INTERVAL = 60            # 自动拉取间隔（秒）

POSITION_RAW_TOKEN = False

# 钱包文件名（默认落到插件 data 目录时使用）
WALLET_FILENAME = "wallet.json"


BIQUOTE_URL = (
    "https://biquote.io/api/latest?symbols=EURUSD&symbols=GBPUSD&symbols=USDJPY&symbols=XAUUSD"
)
_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; AstrBotTokenFX/1.0)"}

BUY_USAGE = (
    "参数不符，请使用/买入 [货币对] [token保证金] [杠杆] [做空/做高] [预定卖出]进行购买\n"
    "例：/买入 EURUSD 100000 20 做高\n"
    "例：/买入 EURUSD 100000 50 做空 20%\n"
    "（预定卖出可选：盈利达到该百分比后自动平仓）"
)
SELL_USAGE = (
    "参数不符，请使用/卖出 [货币对] [比例]进行卖出\n"
    "例：/卖出 EURUSD 50%"
)
TP_USAGE = (
    "参数不符，请使用/止盈 [货币对] [做高/做空] [预定卖出]修改止盈线\n"
    "例：/止盈 EURUSD 做空 20%\n"
    "（同一货币对只有单向持仓时，可省略方向：/止盈 EURUSD 20%）\n"
    "预定卖出填 0 表示取消该笔持仓的止盈"
)

_COMMAND_WORDS = {
    "买入", "buy", "查仓", "positions", "查报价", "quotes", "卖出", "sell", "止盈", "tp", "空中飞人",
}

# token 显示格式化
_TOKEN_UNITS = ["", "Kb", "Mb", "Gb", "Tb", "Pb", "Eb"]


def _format_token(token: int) -> str:
    token = int(token)
    if token < 1024:
        return f"{token} token"
    val = float(token)
    unit_idx = 0
    while val >= 1024.0 and unit_idx < len(_TOKEN_UNITS) - 1:
        val /= 1024.0
        unit_idx += 1
    return f"{val:.2f} {_TOKEN_UNITS[unit_idx]} token"


def _format_position_value(token: int) -> str:
    token = int(token)
    return str(token) if POSITION_RAW_TOKEN else _format_signed_token(token)


def _format_signed_token(token: int) -> str:
    if token < 0:
        return "-" + _format_token(-int(token))
    return _format_token(token)


def _ceil_token(value: float) -> int:
    """所有使用 / 获得的 token 一律向上取整。"""
    return int(math.ceil(float(value)))


def _fmt_pct(value: float) -> str:
    """所有显示的百分比保留两位小数。"""
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.2f}%"


def _to_float(value: Any) -> float:
    try:
        return float(str(value).strip().replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


# 持仓方向：做高 = 买涨，做空 = 买跌
DIR_LONG = "做高"
DIR_SHORT = "做空"


def _norm_direction(text: Any) -> str:
    """把用户输入归一化为「做高 / 做空」，无法识别时返回空串。"""
    raw = str(text or "").strip().lower()
    mapping = {
        "做高": DIR_LONG, "看高": DIR_LONG, "买涨": DIR_LONG, "多": DIR_LONG,
        "做多": DIR_LONG, "涨": DIR_LONG, "多单": DIR_LONG,
        "long": DIR_LONG, "buy": DIR_LONG, "up": DIR_LONG, "bull": DIR_LONG,
        "做空": DIR_SHORT, "看空": DIR_SHORT, "买跌": DIR_SHORT, "空": DIR_SHORT,
        "跌": DIR_SHORT, "空单": DIR_SHORT, "做跌": DIR_SHORT,
        "short": DIR_SHORT, "sell": DIR_SHORT, "down": DIR_SHORT, "bear": DIR_SHORT,
    }
    return mapping.get(raw, "")


def _direction_cn(lot: dict) -> str:
    """老持仓没有 direction 字段时按做高处理，保证升级后不炸。"""
    return DIR_SHORT if lot.get("direction") == DIR_SHORT else DIR_LONG


# ---------------------------------------------------------------------------
# 通用 JSON 文件读写（原子写回）
# ---------------------------------------------------------------------------
def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise RuntimeError(f"读取 {path} 失败：{exc}") from exc


def _atomic_write_json(path: Path, data: Any) -> None:
    """同目录临时文件 + os.replace，保证写回是原子的。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fp:
            json.dump(data, fp, ensure_ascii=False, indent=2)
            fp.flush()
            os.fsync(fp.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# 钱包路径解析（配置驱动，未配置则落到插件 data 目录）
# ---------------------------------------------------------------------------
def _resolve_wallet_path(config: Any, default_data_dir: Path) -> Path:
    raw = ""
    if config is not None:
        if isinstance(config, dict):
            raw = config.get("wallet_path", "") or ""
        else:
            raw = getattr(config, "wallet_path", "") or ""
        raw = str(raw).strip()

    if raw:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = default_data_dir / p
    else:
        p = default_data_dir / WALLET_FILENAME

    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# ---------------------------------------------------------------------------
# wallet.json 跨插件读写（每次强制从文件读取，无任何缓存）
# 注意：wallet_path 与 wallet_lock 由调用方（插件实例）显式传入。
# ---------------------------------------------------------------------------
def _read_wallet(wallet_path: Path) -> dict:
    """文件不存在表示零用户；结构异常时抛错，绝不覆盖损坏的数据。"""
    if not wallet_path.exists():
        return {}
    data = json.loads(wallet_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError("wallet.json 顶层不是对象")
    return data


def _wallet_apply(
    wallet_path: Path,
    wallet_lock: threading.Lock,
    uid: str,
    name: str,
    delta: int = 0,
) -> tuple[int, bool, bool]:
    """
    读 -> （必要时建号）-> 修改 -> 写回，全程持有 wallet_lock。
    返回 (最新余额, 是否新用户, 是否成功)。
    每次调用都重新读取文件，不保留任何 wallet 缓存（规范第九节）。
    """
    with wallet_lock:
        try:
            data = _read_wallet(wallet_path)
        except Exception as exc:
            logger.error("[token_fx] 读取钱包失败：%s", exc)
            return 0, False, False

        is_new = uid not in data
        if is_new:
            # 规范第二节：新用户自动建号，初始 token = 500000
            data[uid] = {
                "user_id": uid,
                "name": name or uid,
                "token": NEW_USER_BONUS,
            }

        record = data[uid]
        if not isinstance(record, dict):
            logger.error("[token_fx] 用户 %s 的钱包记录异常", uid)
            return 0, False, False

        if name:
            record["name"] = name
        record["user_id"] = uid

        try:
            balance = int(record.get("token", 0))
        except (TypeError, ValueError):
            balance = 0

        new_balance = balance + int(delta)
        record["token"] = new_balance  # 本地存储仍为十进制整数

        if is_new or delta != 0:
            try:
                _atomic_write_json(wallet_path, data)
            except Exception:
                logger.exception("[token_fx] 写回 wallet.json 失败")
                return 0, False, False

        return new_balance, is_new, True


# ---------------------------------------------------------------------------
# 持仓与报价的数学口径（所有 token 金额在输出 / 入账时向上取整）
# ---------------------------------------------------------------------------
def _lot_price_change(lot: dict, price: float) -> float:
    """带方向的价格变动率：做高 = 现价/开仓价-1，做空 = 1-现价/开仓价。"""
    open_price = float(lot["open_price"])
    if open_price <= 0:
        return 0.0
    ratio = float(price) / open_price - 1.0
    return ratio if _direction_cn(lot) == DIR_LONG else -ratio


def _lot_equity(lot: dict, price: float) -> float:
    """
    按当前报价平仓后可得的 token（权益，可能为负）。
    equity = 保证金 + 名义本金 × 带方向的价格变动率
    盈亏比例 = (equity - 保证金) / 保证金 = 杠杆 × 带方向的价格变动率
    """
    notional = float(lot["notional"])
    margin = float(lot["margin"])
    return margin + notional * _lot_price_change(lot, price)


def _lot_pnl_pct(lot: dict, price: float) -> float:
    margin = float(lot["margin"])
    if margin <= 0:
        return 0.0
    return (_lot_equity(lot, price) - margin) / margin * 100.0


# ---------------------------------------------------------------------------
# 插件主体
# ---------------------------------------------------------------------------
class TokenFXPlugin(Star):
    # ★ 类级共享锁：所有插件实例共用同一把钱包锁，避免多实例并发写坏 wallet.json
    _wallet_lock = threading.Lock()

    def __init__(self, context: Context, config: Any = None):
        super().__init__(context, config)
        self._data_dir = self._resolve_data_dir()
        # ★ 钱包路径：配置优先，未配置则落到插件 data 目录
        self.wallet_path = _resolve_wallet_path(config, self._data_dir)
        logger.info("[token_fx] 钱包文件路径：%s", self.wallet_path)

        self._state_path = self._data_dir / "fx_state.json"
        self._data_lock = threading.Lock()
        self._quote_lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

        raw = _read_json(self._state_path, {})
        if not isinstance(raw, dict):
            raw = {}
        self._data = {
            "positions": raw.get("positions") if isinstance(raw.get("positions"), dict) else {},
            "quotes": raw.get("quotes") if isinstance(raw.get("quotes"), dict) else {},
            "quote_source": raw.get("quote_source", ""),
            "quote_time": raw.get("quote_time", ""),
            "user_umo": raw.get("user_umo") if isinstance(raw.get("user_umo"), dict) else {},
        }
        # ---- 启动后台行情循环（不依赖 on_astrbot_loaded 的触发时机）----
        self._start_quote_loop()

    # ------------------------------------------------------------------
    # 数据目录（StarTools.get_data_dir() 返回本插件的 data/plugin_data 目录）
    # ------------------------------------------------------------------
    def _resolve_data_dir(self) -> Path:
        candidates = [
            lambda: Path(StarTools.get_data_dir()),
            lambda: Path(StarTools.get_data_dir(getattr(self, "name", "astrbot_plugin_fx"))),
            lambda: Path("data/plugin_data/astrbot_plugin_fx"),
        ]
        for factory in candidates:
            try:
                path = factory()
                path.mkdir(parents=True, exist_ok=True)
                return path
            except Exception:
                continue
        return Path("data/plugin_data/astrbot_plugin_fx")

    def _save_state_locked(self) -> None:
        """调用方必须已持有 self._data_lock。"""
        _atomic_write_json(self._state_path, self._data)

    def _save_state(self) -> None:
        with self._data_lock:
            self._save_state_locked()

    def _record_umo(self, uid: str, umo: str) -> None:
        """记录用户最近一次使用插件的会话，用于爆仓消息按群合并推送。"""
        if not umo:
            return
        with self._data_lock:
            self._data["user_umo"][uid] = umo
            for lot in self._data["positions"].get(uid, []):
                lot["umo"] = umo
            self._save_state_locked()

    def _quotes_snapshot(self) -> dict:
        with self._data_lock:
            return dict(self._data["quotes"])

    # ------------------------------------------------------------------
    # 钱包读写快捷方法（把实例的路径与锁传给模块级函数）
    # ------------------------------------------------------------------
    def _wallet_apply(self, uid: str, name: str, delta: int = 0) -> tuple[int, bool, bool]:
        return _wallet_apply(self.wallet_path, self._wallet_lock, uid, name, delta)

    # ------------------------------------------------------------------
    # 真实报价拉取（多源回退，全部无需 API key）
    # ------------------------------------------------------------------
    async def _http_text(self, session: aiohttp.ClientSession, url: str) -> str:
        async with session.get(
            url, headers=_HEADERS, timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            resp.raise_for_status()
            return await resp.text()

    async def _http_json(self, session: aiohttp.ClientSession, url: str) -> Any:
        return json.loads(await self._http_text(session, url))

    async def _q_biquote(self, session: aiohttp.ClientSession) -> dict:
        """
        biquote 批量报价：一次请求拿四个品种。
        返回 { "EURUSD": 1.08542, ... }，只取 mid。
        """
        out: dict[str, float] = {}
        try:
            data = await self._http_json(session, BIQUOTE_URL)
        except Exception as exc:
            logger.warning("[token_fx] biquote 拉取失败：%s", exc)
            return out

        if not isinstance(data, dict):
            return out

        for pair in PAIRS:
            item = data.get(pair)
            if not isinstance(item, dict):
                continue
            # 优先用 mid；顺便看市场状态，闭市只当兜底
            price = _to_float(item.get("mid"))
            if price <= 0:
                continue
            out[pair] = price

        return out

    async def _q_exchangerate_fun(self, session: aiohttp.ClientSession) -> dict:
        """备用源：USD 基准汇率表（含 XAU），一次请求拿全四组。"""
        data = await self._http_json(
            session, "https://api.exchangerate.fun/latest?base=USD"
        )
        rates = data.get("rates") or {} if isinstance(data, dict) else {}
        out: dict[str, float] = {}

        def inverse(code: str) -> float:
            value = _to_float(rates.get(code))
            return 1.0 / value if value > 0 else 0.0

        for pair, code in (("EURUSD", "EUR"), ("GBPUSD", "GBP")):
            value = inverse(code)
            if value > 0:
                out[pair] = value
        jpy = _to_float(rates.get("JPY"))
        xau = _to_float(rates.get("XAU"))
        if jpy > 0:
            out["USDJPY"] = jpy
        if xau > 0:
            out["XAUUSD"] = 1.0 / xau
        return out

    async def _q_frankfurter(self, session: aiohttp.ClientSession) -> dict:
        """备用源：Frankfurter v2 法币参考汇率 + Gold-API 现货黄金。"""
        rows = await self._http_json(
            session,
            "https://api.frankfurter.dev/v2/rates?base=USD&quotes=EUR,GBP,JPY",
        )
        if isinstance(rows, list):
            rate_map = {
                str(row.get("quote", "")).upper(): _to_float(row.get("rate"))
                for row in rows
                if isinstance(row, dict)
            }
        elif isinstance(rows, dict):
            # 兼容 v1 风格 {"base": "USD", "rates": {"EUR": 0.89, ...}}
            raw_rates = rows.get("rates") or {}
            rate_map = {
                str(code).upper(): _to_float(value)
                for code, value in raw_rates.items()
            } if isinstance(raw_rates, dict) else {}
        else:
            rate_map = {}

        gold = await self._http_json(session, "https://api.gold-api.com/price/XAU")
        gold_price = _to_float(gold.get("price")) if isinstance(gold, dict) else 0.0

        out: dict[str, float] = {}
        for pair, code in (("EURUSD", "EUR"), ("GBPUSD", "GBP")):
            value = rate_map.get(code, 0.0)
            if value > 0:
                out[pair] = 1.0 / value  # USD->XXX 取倒数得到 XXX/USD
        if rate_map.get("JPY", 0.0) > 0:
            out["USDJPY"] = rate_map["JPY"]
        if gold_price > 0:
            out["XAUUSD"] = gold_price
        return out

    async def _q_erapi(self, session: aiohttp.ClientSession) -> dict:
        """最后兜底：open.er-api.com + goldprice.dev。"""
        data = await self._http_json(
            session, "https://open.er-api.com/v6/latest/USD"
        )
        rates = data.get("rates") or {} if isinstance(data, dict) else {}
        gold = await self._http_json(
            session, "https://api.goldprice.dev/v1/prices?symbol=XAU-USD-SPOT"
        )
        symbols = gold.get("symbols") or [] if isinstance(gold, dict) else []
        gold_price = (
            _to_float(symbols[0].get("price"))
            if symbols and isinstance(symbols[0], dict)
            else 0.0
        )
        out: dict[str, float] = {}
        for pair, code in (("EURUSD", "EUR"), ("GBPUSD", "GBP")):
            value = _to_float(rates.get(code))
            if value > 0:
                out[pair] = 1.0 / value
        jpy = _to_float(rates.get("JPY"))
        if jpy > 0:
            out["USDJPY"] = jpy
        if gold_price > 0:
            out["XAUUSD"] = gold_price
        return out

    async def _fetch_quotes(self) -> tuple[dict[str, float], str] | None:
        """
        依次尝试各数据源，凑齐四组报价；个别缺失的品种沿用上次成功的价格。
        全部凑不齐时返回 None（本次不更新、不做爆仓判定）。
        """
        merged: dict[str, float] = {}
        used_source = ""
        fetchers = (
            ("biquote", self._q_biquote),
            ("exchangerate.fun", self._q_exchangerate_fun),
            ("frankfurter+gold-api", self._q_frankfurter),
            ("er-api+goldprice.dev", self._q_erapi),
        )
        async with aiohttp.ClientSession() as session:
            for label, fetcher in fetchers:
                try:
                    part = await fetcher(session)
                except Exception as exc:
                    logger.warning("[token_fx] 报价源 %s 拉取失败：%s", label, exc)
                    continue
                if not isinstance(part, dict):
                    continue
                added = 0
                for pair, price in part.items():
                    if (
                        pair in PAIRS
                        and isinstance(price, (int, float))
                        and price > 0
                        and pair not in merged
                    ):
                        merged[pair] = float(price)
                        added += 1
                if added:
                    used_source = used_source or label
                if len(merged) == len(PAIRS):
                    break

        cached = self._quotes_snapshot()
        for pair in PAIRS:
            if pair not in merged:
                value = cached.get(pair)
                if isinstance(value, (int, float)) and value > 0:
                    merged[pair] = float(value)

        if len(merged) < len(PAIRS):
            return None
        return ({pair: merged[pair] for pair in PAIRS}, used_source)

    # ------------------------------------------------------------------
    # 拉取 + 爆仓检查（每次拉取报价后必定执行）
    # ------------------------------------------------------------------
    async def _refresh_and_check(self) -> tuple[bool, int, int]:
        """
        拉取报价 -> 写盘 -> 爆仓检查 -> 止盈检查。
        返回 (是否刷新成功, 强平笔数, 止盈平仓笔数)。
        """
        async with self._quote_lock:
            try:
                result = await asyncio.wait_for(self._fetch_quotes(), timeout=45)
            except asyncio.TimeoutError:
                logger.warning("[token_fx] 拉取报价整体超时（45s），本次跳过")
                return False, 0, 0
            except Exception:
                logger.exception("[token_fx] 拉取报价异常")
                return False, 0, 0
            if result is None:
                logger.warning("[token_fx] 四组报价均未取到，本次跳过爆仓检查")
                return False, 0, 0
            quotes, source = result
            with self._data_lock:
                self._data["quotes"] = quotes
                self._data["quote_source"] = source
                self._data["quote_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self._save_state_locked()
            liquidated = await self._check_liquidation()
            tp_closed = await self._check_take_profit()
            return True, liquidated, tp_closed

    async def _check_liquidation(self) -> int:
        """
        按 (用户, 货币对) 汇总权益；权益 <= 0 即爆仓：
        强平该货币对全部持仓，并把不足部分从钱包账户中等额扣除。
        同一会话（群）所有人的爆仓信息合并成一条消息推送。
        返回本次强平的持仓笔数。
        """
        with self._data_lock:
            snapshot = json.loads(json.dumps(self._data["positions"], ensure_ascii=False))
            quotes = dict(self._data["quotes"])
            umo_map = dict(self._data["user_umo"])

        messages: dict[str, list[str]] = {}
        to_remove: dict[str, set[str]] = {}
        liquidated_lots = 0  # 本次强平的持仓笔数

        for uid, lots in snapshot.items():
            if not isinstance(lots, list):
                continue
            by_pair: dict[str, list[dict]] = {}
            for lot in lots:
                if not isinstance(lot, dict):
                    continue
                pair = str(lot.get("pair", "")).upper()
                if pair in PAIRS:
                    by_pair.setdefault(pair, []).append(lot)

            for pair, group in by_pair.items():
                price = quotes.get(pair)
                if not price or price <= 0:
                    continue
                total_equity = sum(_lot_equity(lot, price) for lot in group)
                if total_equity > 0:
                    continue
                user_name = str(group[0].get("name") or uid)
                shortfall = _ceil_token(-total_equity)  # 扣除的 token 向上取整
                _, _, ok = self._wallet_apply(uid, user_name, -shortfall)
                if not ok:
                    logger.error("[token_fx] 爆仓扣款失败：uid=%s pair=%s", uid, pair)
                umo = umo_map.get(uid) or str(group[0].get("umo") or "")
                messages.setdefault(umo, []).append(
                    f"{user_name}持有的{pair}爆仓了，已强制平仓。"
                )
                to_remove.setdefault(uid, set()).add(pair)
                liquidated_lots += len(group)

        if to_remove:
            with self._data_lock:
                for uid, pairs in to_remove.items():
                    remain = [
                        lot
                        for lot in self._data["positions"].get(uid, [])
                        if lot.get("pair") not in pairs
                    ]
                    if remain:
                        self._data["positions"][uid] = remain
                    else:
                        self._data["positions"].pop(uid, None)
                self._save_state_locked()

        for umo, lines in messages.items():
            if not umo:
                logger.warning("[token_fx] 无会话来源，爆仓消息未推送：%s", " / ".join(lines))
                continue
            try:
                await self.context.send_message(
                    umo, MessageChain().message("\n".join(lines))
                )
            except Exception:
                logger.exception("[token_fx] 爆仓消息推送失败：%s", umo)

        return liquidated_lots

    async def _check_take_profit(self) -> int:
        """
        逐笔检查：盈亏比例 >= take_profit 的持仓自动平仓，
        卖出所得（权益）入账，并按会话（群）合并推送通知。
        爆仓已先行处理，此处命中的盈亏必然为正。
        返回本次止盈平仓的持仓笔数。
        """
        with self._data_lock:
            snapshot = json.loads(json.dumps(self._data["positions"], ensure_ascii=False))
            quotes = dict(self._data["quotes"])
            umo_map = dict(self._data["user_umo"])

        # uid -> 待移除的 lot_id 集合
        to_remove: dict[str, set[str]] = {}
        # umo -> 通知行
        messages: dict[str, list[str]] = {}
        tp_lots = 0  # 本次止盈平仓的持仓笔数

        for uid, lots in snapshot.items():
            if not isinstance(lots, list):
                continue
            for lot in lots:
                if not isinstance(lot, dict):
                    continue
                tp = _to_float(lot.get("take_profit", 0))
                if tp <= 0:
                    continue
                pair = str(lot.get("pair", "")).upper()
                price = quotes.get(pair)
                if not isinstance(price, (int, float)) or price <= 0:
                    continue
                if _lot_pnl_pct(lot, price) < tp:
                    continue

                equity = _ceil_token(_lot_equity(lot, price))
                profit = equity - int(lot.get("margin", 0))
                user_name = str(lot.get("name") or uid)
                _, _, ok = self._wallet_apply(uid, user_name, equity)
                if not ok:
                    logger.error("[token_fx] 止盈入账失败：uid=%s pair=%s", uid, pair)
                    continue

                umo = umo_map.get(uid) or str(lot.get("umo") or "")
                sign = "获利" if profit >= 0 else "亏损"
                messages.setdefault(umo, []).append(
                    f"{user_name}的{pair}已达到预定卖出 {tp:.2f}%，"
                    f"已自动平仓，{sign}{_format_token(abs(profit))}。"
                )
                to_remove.setdefault(uid, set()).add(str(lot.get("lot_id")))
                tp_lots += 1

        if to_remove:
            with self._data_lock:
                for uid, lot_ids in to_remove.items():
                    remain = [
                        lot
                        for lot in self._data["positions"].get(uid, [])
                        if str(lot.get("lot_id")) not in lot_ids
                    ]
                    if remain:
                        self._data["positions"][uid] = remain
                    else:
                        self._data["positions"].pop(uid, None)
                self._save_state_locked()

        for umo, lines in messages.items():
            if not umo:
                logger.warning("[token_fx] 无会话来源，止盈消息未推送：%s", " / ".join(lines))
                continue
            try:
                await self.context.send_message(
                    umo, MessageChain().message("\n".join(lines))
                )
            except Exception:
                logger.exception("[token_fx] 止盈消息推送失败：%s", umo)

        return tp_lots

    # ------------------------------------------------------------------
    # 生命周期：启动时立即拉取一次，之后每分钟拉取一次
    # ------------------------------------------------------------------
    def _start_quote_loop(self) -> None:
        """
        幂等启动后台行情循环。
        __init__ 里直接调用一次，不依赖 on_astrbot_loaded 的触发时机
        （某些框架版本下该钩子不触发或晚于预期，会导致循环从来没跑起来）。
        """
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = None
        if loop is None or loop.is_closed():
            logger.warning("[token_fx] 无可用事件循环，行情循环未启动")
            return
        if self._task is not None and not self._task.done():
            logger.info("[token_fx] 行情循环已在运行，跳过重复启动")
            return
        self._task = loop.create_task(self._quote_loop())
        logger.info("[token_fx] 行情循环已启动，间隔 %d 秒", QUOTE_INTERVAL)

    @filter.on_astrbot_loaded()
    async def on_astrbot_loaded(self):
        # 真正启动逻辑在 _start_quote_loop（__init__ 已调用）。
        # 这里只做兜底：万一 __init__ 时事件循环还没就绪，此处补启；
        # _start_quote_loop 内部幂等，不会重复创建任务。
        self._start_quote_loop()

    async def _refresh_quotes_silently(self) -> bool:
        """
        自动循环专用：等价于 /查报价 的刷新部分，但：
        - 不记录 umo
        - 不建号 / 不发新手奖励
        - 不发送任何报价消息
        刷新 + 写盘 + 爆仓/止盈检查全部完成后，打印一条 logger.info。
        返回是否成功刷新（成功即已落盘）。
        """
        ok, liquidated, tp_closed = await self._refresh_and_check()
        if not ok:
            logger.warning("[token_fx] 自动刷新未成功，本次不更新存档")
            return False
        quotes = self._quotes_snapshot()
        with self._data_lock:
            quote_time = str(self._data.get("quote_time", ""))
            source = str(self._data.get("quote_source", ""))
        price_txt = " ".join(
            f"{pair}={quotes[pair]:.{PRICE_DIGITS[pair]}f}"
            for pair in PAIRS
            if isinstance(quotes.get(pair), (int, float)) and quotes[pair] > 0
        )

        return True

    async def _quote_loop(self) -> None:
        """后台常驻：每分钟静默刷新报价 + 检查爆仓/止盈，不发送报价消息。"""
        logger.info("[token_fx] 行情循环进入运行态")
        while True:
            try:
                await self._refresh_quotes_silently()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[token_fx] 定时刷新报价异常")
            await asyncio.sleep(QUOTE_INTERVAL)

    async def terminate(self):
        """插件卸载 / 重载时取消后台任务，避免重复定时器。"""
        if self._task is not None:
            logger.info("[token_fx] 正在停止行情循环")
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            except Exception:
                logger.exception("[token_fx] 停止行情循环异常")
            self._task = None
            logger.info("[token_fx] 行情循环已停止")

    # ------------------------------------------------------------------
    # 指令公共部分
    # ------------------------------------------------------------------
    @staticmethod
    def _who(event: AstrMessageEvent) -> tuple[str, str, str]:
        uid = str(event.get_sender_id())
        name = event.get_sender_name() or uid
        umo = str(event.unified_msg_origin or "")
        return uid, name, umo

    @staticmethod
    def _args(event: AstrMessageEvent) -> list[str]:
        """从消息文本解析参数（不依赖函数签名注入，规范第六节）。"""
        text = (event.message_str or "").replace("／", "/").strip()
        parts = text.split()
        if parts and parts[0].lstrip("/").lower() in _COMMAND_WORDS:
            parts = parts[1:]
        return parts

    # ------------------------------------------------------------------
    # /买入 [货币对] [token保证金] [杠杆] [做空/做高]
    # ------------------------------------------------------------------
    @filter.command("买入", alias={"buy"})
    async def cmd_buy(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        args = self._args(event)
        if len(args) < 4:
            yield event.plain_result(BUY_USAGE)
            return

        pair = args[0].upper()
        margin_raw = _to_float(args[1])
        leverage = _to_float(args[2])
        direction = _norm_direction(args[3])

        # 第 5 个参数：预定卖出百分比（可选，0 或省略表示不止盈）
        take_profit = 0.0
        if len(args) >= 5:
            take_profit = _to_float(re.sub(r"[%％]", "", args[4]))
            if take_profit <= 0:
                yield event.plain_result(BUY_USAGE)
                return

        if pair not in PAIRS or margin_raw <= 0 or leverage < 1 or not direction:
            yield event.plain_result(BUY_USAGE)
            return

        margin = _ceil_token(margin_raw)  # 使用的 token 向上取整
        self._record_umo(uid, umo)

        # 每次买入操作都拉取一次报价（拉取后自动检查爆仓）
        refreshed, _, _ = await self._refresh_and_check()
        quotes = self._quotes_snapshot()
        if not refreshed and pair not in quotes:
            yield event.plain_result("报价获取失败，请稍后再试")
            return
        price = quotes.get(pair)
        if not price or price <= 0:
            yield event.plain_result(f"{pair} 暂无报价，请稍后再试")
            return

        # 建号 / 新手奖励（每次都从 wallet.json 重新读取）
        balance, is_new, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        if is_new:
            yield event.plain_result("是新用户！已发放新手奖励")
            yield event.plain_result(f"+{_format_token(NEW_USER_BONUS)}")

        if balance < margin:
            yield event.plain_result(f"余额不足，当前 {_format_signed_token(balance)}")
            return

        _, _, ok = self._wallet_apply(uid, name, -margin)
        if not ok:
            yield event.plain_result("钱包写入失败，请稍后再试")
            return

        notional = _ceil_token(margin * leverage)  # 名义本金（token，向上取整）
        qty = notional / (price * TOKEN_PER_USD)
        lot = {
            "lot_id": f"{uid}-{pair}-{time.time_ns()}",
            "pair": pair,
            "direction": direction,          # 新增：做高 / 做空
            "qty": qty,
            "margin": margin,
            "leverage": leverage,
            "notional": notional,
            "open_price": price,
            "take_profit": take_profit,      # 新增：>0 表示盈利达该百分比自动平仓
            "name": name,
            "umo": umo,
        }
        with self._data_lock:
            self._data["positions"].setdefault(uid, []).append(lot)
            self._data["user_umo"][uid] = umo
            self._save_state_locked()

        remain, _, _ = self._wallet_apply(uid, name, 0)
        tp_line = (
            f"预定卖出：盈利达到 {take_profit:.2f}% 自动平仓\n"
            if take_profit > 0 else ""
        )
        yield event.plain_result(
            "买入成功！\n"
            f"{pair} {direction}\n持有：{qty:.2f}({leverage:g}倍杠杆)\n"
            f"支付保证金：{_format_token(margin)}\n"
            f"{tp_line}"
            f"当前余额：{_format_signed_token(remain)}"
        )

    # ------------------------------------------------------------------
    # /查仓
    # ------------------------------------------------------------------
    @filter.command("查仓", alias={"positions"})
    async def cmd_positions(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        self._record_umo(uid, umo)

        _, is_new, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        if is_new:
            yield event.plain_result("是新用户！已发放新手奖励")
            yield event.plain_result(f"+{_format_token(NEW_USER_BONUS)}")

        # ★ 查仓时拉一次实时报价，并顺带做爆仓检查
        refreshed, _, _ = await self._refresh_and_check()
        quotes = self._quotes_snapshot()

        with self._data_lock:
            lots = [
                dict(lot)
                for lot in self._data["positions"].get(uid, [])
                if isinstance(lot, dict)
            ]
            quote_time = self._data.get("quote_time", "")

        if not lots:
            yield event.plain_result("当前没有持仓")
            return

        # 报价未更新时明确提示，避免用户误以为看到的是实时盈亏
        stale_pairs: list[str] = []
        lines = ["当前持仓："]
        for lot in lots:
            pair = str(lot.get("pair", "")).upper()
            price = quotes.get(pair)
            if not refreshed or not isinstance(price, (int, float)) or price <= 0:
                # 无实时价：不伪造盈亏，明确标记
                stale_pairs.append(pair)
                leverage = float(lot.get("leverage", 1))
                qty = float(lot.get("qty", 0))
                tp = _to_float(lot.get("take_profit", 0))
                tp_txt = f"预定卖出：{tp:.2f}%\n" if tp > 0 else ""
                lines.append(
                    f"{pair} {_direction_cn(lot)}\n持有：{qty:.2f}({leverage:g}倍杠杆)\n{tp_txt}"
                    f"卖出获得：暂无实时报价\n盈亏：暂无实时报价"
                )
                continue
            equity = _ceil_token(_lot_equity(lot, price))
            pnl = _lot_pnl_pct(lot, price)
            leverage = float(lot.get("leverage", 1))
            qty = float(lot.get("qty", 0))
            tp = _to_float(lot.get("take_profit", 0))
            tp_txt = f"预定卖出：{tp:.2f}%\n" if tp > 0 else ""
            lines.append(
                f"{pair} {_direction_cn(lot)}\n持有：{qty:.2f}({leverage:g}倍杠杆)\n{tp_txt}"
                f"卖出获得：{_format_position_value(equity)}\n盈亏：{_fmt_pct(pnl)}"
            )

        if stale_pairs:
            lines.append(
                f"（{'/'.join(sorted(set(stale_pairs)))} 本次报价未更新，"
                f"上次报价时间：{quote_time or '无'}）"
            )
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # /查报价：输出四个货币对，严格四行
    # ------------------------------------------------------------------
    @filter.command("查报价", alias={"quotes"})
    async def cmd_quotes(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        self._record_umo(uid, umo)

        _, is_new, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        if is_new:
            yield event.plain_result("是新用户！已发放新手奖励")
            yield event.plain_result(f"+{_format_token(NEW_USER_BONUS)}")

        # 查报价也拉取一次最新报价（拉取后自动检查爆仓）
        refreshed, _, _ = await self._refresh_and_check()
        quotes = self._quotes_snapshot()
        if not refreshed and not quotes:
            yield event.plain_result("报价获取失败，请稍后再试")
            return

        lines = []
        for pair in PAIRS:
            value = quotes.get(pair)
            if isinstance(value, (int, float)) and value > 0:
                lines.append(f"{pair} {value:.{PRICE_DIGITS[pair]}f}")
            else:
                lines.append(f"{pair} 暂无报价")
        yield event.plain_result("\n".join(lines))

    # ------------------------------------------------------------------
    # /卖出 [货币对] [比例]（比例用百分数，未指定则全部卖出）
    # ------------------------------------------------------------------
    @filter.command("卖出", alias={"sell"})
    async def cmd_sell(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        args = self._args(event)
        if not args or args[0].upper() not in PAIRS:
            yield event.plain_result(SELL_USAGE)
            return

        pair = args[0].upper()
        ratio = 100.0
        if len(args) >= 2:
            ratio = _to_float(re.sub(r"[%％]", "", args[1]))
            if ratio <= 0 or ratio > 100:
                yield event.plain_result(SELL_USAGE)
                return

        self._record_umo(uid, umo)

        _, is_new, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        if is_new:
            yield event.plain_result("是新用户！已发放新手奖励")
            yield event.plain_result(f"+{_format_token(NEW_USER_BONUS)}")

        # 每次卖出操作都拉取一次报价（拉取后自动检查爆仓）
        refreshed, _, _ = await self._refresh_and_check()
        quotes = self._quotes_snapshot()
        if not refreshed and not quotes:
            yield event.plain_result("报价获取失败，请稍后再试")
            return

        with self._data_lock:
            own_lots = [
                lot
                for lot in self._data["positions"].get(uid, [])
                if isinstance(lot, dict) and lot.get("pair") == pair
            ]
        if not own_lots:
            yield event.plain_result("当前没有持仓")
            return

        price = quotes.get(pair)
        if not isinstance(price, (int, float)) or price <= 0:
            yield event.plain_result(f"{pair} 暂无实时报价，请稍后再试")
            return

        fraction = ratio / 100.0
        total_gain = 0
        total_margin = 0  # 本次卖出部分对应的保证金本金，用于计算真实盈亏

        with self._data_lock:
            all_lots = self._data["positions"].get(uid, [])
            others = [lot for lot in all_lots if lot.get("pair") != pair]
            kept: list[dict] = []
            for lot in all_lots:
                if lot.get("pair") != pair:
                    continue
                equity = _lot_equity(lot, price)
                partial = equity * fraction
                # 获得 / 扣除的 token 都向上取整
                total_gain += (
                    _ceil_token(partial) if partial >= 0 else -_ceil_token(-partial)
                )
                # 本次卖出部分对应的保证金（按比例分摊，向上取整）
                total_margin += _ceil_token(float(lot["margin"]) * fraction)

                if fraction >= 1.0:
                    continue
                rest = 1.0 - fraction
                lot["qty"] = float(lot["qty"]) * rest
                lot["margin"] = _ceil_token(float(lot["margin"]) * rest)
                lot["notional"] = _ceil_token(float(lot["notional"]) * rest)
                kept.append(lot)

            if others or kept:
                self._data["positions"][uid] = others + kept
            else:
                self._data["positions"].pop(uid, None)
            self._save_state_locked()

        balance, _, ok = self._wallet_apply(uid, name, total_gain)
        if not ok:
            yield event.plain_result("钱包写入失败，请稍后再试")
            return

        # 本次真实盈亏 = 卖出所得 - 卖出部分对应的保证金
        profit = total_gain - total_margin
        if profit >= 0:
            pnl_line = f"本次共获利{_format_token(profit)}"
        else:
            pnl_line = f"本次共损失{_format_token(-profit)}"

        yield event.plain_result(
            "卖出成功！\n"
            f"{pair} 已卖出 {ratio:.2f}%\n"
            f"卖出获得：{_format_signed_token(total_gain)}\n"
            f"{pnl_line}\n"
            f"当前余额：{_format_signed_token(balance)}"
        )

    # ------------------------------------------------------------------
    # /止盈 [货币对] [做高/做空] [预定卖出]
    #   - 同一货币对同时有做高、做空两笔时必须指定方向
    #   - 只有单向持仓时可省略方向
    #   - 预定卖出填 0 表示取消止盈
    # ------------------------------------------------------------------
    @filter.command("止盈", alias={"tp"})
    async def cmd_take_profit(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        args = self._args(event)
        if not args or args[0].upper() not in PAIRS:
            yield event.plain_result(TP_USAGE)
            return

        pair = args[0].upper()

        # 解析方向（可选）与预定卖出（必填），兼容两种参数顺序：
        #   /止盈 EURUSD 20%         -> 省略方向
        #   /止盈 EURUSD 做空 20%    -> 指定方向
        direction = ""
        tp_raw: Any = None
        if len(args) >= 2:
            maybe_dir = _norm_direction(args[1])
            if maybe_dir:
                direction = maybe_dir
                if len(args) >= 3:
                    tp_raw = args[2]
            else:
                tp_raw = args[1]
        if tp_raw is None:
            yield event.plain_result(TP_USAGE)
            return

        take_profit = _to_float(re.sub(r"[%％]", "", str(tp_raw)))
        if take_profit < 0:
            yield event.plain_result(TP_USAGE)
            return

        self._record_umo(uid, umo)

        _, is_new, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        if is_new:
            yield event.plain_result("是新用户！已发放新手奖励")
            yield event.plain_result(f"+{_format_token(NEW_USER_BONUS)}")

        with self._data_lock:
            own_lots = [
                lot
                for lot in self._data["positions"].get(uid, [])
                if isinstance(lot, dict) and lot.get("pair") == pair
            ]
        if not own_lots:
            yield event.plain_result(f"你没有 {pair} 的持仓")
            return

        # 该货币对下存在哪些方向
        dirs_present = {_direction_cn(lot) for lot in own_lots}

        # 需要指定方向却没指定：同时有做高和做空
        if not direction and len(dirs_present) > 1:
            yield event.plain_result(
                f"你同时持有 {pair} 的做高和做空仓位，请指定方向：\n"
                f"例：/止盈 {pair} 做高 {take_profit:.2f}%\n"
                f"例：/止盈 {pair} 做空 {take_profit:.2f}%"
            )
            return

        # 指定了方向但没有对应持仓
        if direction and direction not in dirs_present:
            yield event.plain_result(f"你没有 {pair} {direction} 的持仓")
            return

        # 省略方向且只有单一方向：自动锁定该方向
        if not direction:
            direction = next(iter(dirs_present))

        # 写入止盈线
        with self._data_lock:
            changed = 0
            for lot in self._data["positions"].get(uid, []):
                if not isinstance(lot, dict):
                    continue
                if lot.get("pair") != pair:
                    continue
                if _direction_cn(lot) != direction:
                    continue
                lot["take_profit"] = take_profit
                changed += 1
            self._save_state_locked()

        if changed <= 0:
            yield event.plain_result(f"没有可修改的 {pair} {direction} 持仓")
            return

        if take_profit <= 0:
            yield event.plain_result(
                f"已取消 {pair} {direction} 共 {changed} 笔持仓的止盈"
            )
            return

        yield event.plain_result(
            f"已设置 {pair} {direction} 共 {changed} 笔持仓的止盈\n"
            f"预定卖出：盈利达到 {take_profit:.2f}% 自动平仓"
        )

    # ------------------------------------------------------------------
    # /空中飞人：清空持仓 + 清零余额
    # ------------------------------------------------------------------
    @filter.command("空中飞人")
    async def cmd_freefall(self, event: AstrMessageEvent):
        uid, name, umo = self._who(event)
        self._record_umo(uid, umo)

        # 先清持仓
        with self._data_lock:
            lots = self._data["positions"].pop(uid, [])
            self._save_state_locked()

        # 再清零钱包余额（delta = -当前余额）
        balance, _, ok = self._wallet_apply(uid, name, 0)
        if not ok:
            yield event.plain_result("钱包数据异常，请稍后再试")
            return
        _, _, ok = self._wallet_apply(uid, name, -balance)
        if not ok:
            yield event.plain_result("钱包写入失败，请稍后再试")
            return

        cleared = len(lots) if isinstance(lots, list) else 0
        yield event.plain_result(
            "再见了，这美好的世界\n"
            f"已清空持仓 {cleared} 笔，余额已清零"
        )
