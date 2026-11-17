# astrbot_plugin_fx · Token 外汇交易插件


## 安装

1. 确认已安装并启用 `astrbot_plugin_token_wallet`（版本 ≥ 1.0.3），在配置列表中将文件目录修改为wallet插件同文件下。
2. 把本目录放到 `data/plugins/astrbot_plugin_fx/`（目录名需与 `metadata.yaml` 的 `name` 一致）。
3. 依赖会自动按 `requirements.txt` 安装（仅 `aiohttp`）；也可手动 `pip install aiohttp`。
4. 在 WebUI 插件管理里点「重载插件」。加载完成后立即拉取一次报价，之后每 60 秒自动拉取一次。

> 平台：QQ NapCat（aiocqhttp）。主动推送爆仓消息依赖平台支持主动消息，OneBot v11 可用。

## 指令

| 指令 | 说明 |
|---|---|
| `/买入 [货币对] [token保证金] [杠杆]` | 例：`/买入 EURUSD 100000 20`；参数不完整输出规定的「参数不符咕…」提示 |
| `/查仓` | 无持仓输出「当前没有持仓」；有持仓按指定格式逐行输出 |
| `/查报价` | 输出 EURUSD / GBPUSD / USDJPY / XAUUSD 四行报价 |
| `/卖出 [货币对] [比例]` | 例：`/卖出 EURUSD 50%`；比例用百分数，省略则全部卖出 |

英文别名：`/buy`、`/positions`、`/quotes`、`/sell`。

### 查仓输出格式

```
当前持仓：
EURUSD 持有：123.45(100倍杠杆) 卖出获得：1.78 Mb token 盈亏：-16.83%
……
```

- 「盈亏」= `(权益 - 保证金) / 保证金`，所有百分比保留两位小数。
- 「卖出获得」是**按当前报价全部平仓后实际到账的 token（权益）**，不是名义市值；
  它与盈亏比例自洽：`卖出获得 ≈ 保证金 × (1 + 盈亏比例)`。
  默认所有 token 展示都经 `_format_token()`（例如 `1.78 Mb token`）。
  如需严格复刻原始整数，把 `main.py` 顶部 `POSITION_RAW_TOKEN` 改为 `True`。

## 计价与计算口径（10000 token = 1 USD）

开仓（保证金 `M`、杠杆 `L`、开仓价 `P0`）：

- 从钱包扣除 `M`（向上取整），名义本金 `N = ceil(M × L)` token
- 持有量 `Q = N / (P0 × 10000)`
- 当前平仓价值 `G = N × (现价 / P0)`，借款 `B = N - M`
- **权益（卖出获得）** `E = G - B = M + N × (现价 / P0 - 1)`
- **盈亏比例** `(E - M) / M × 100% = L × (现价 / P0 - 1) × 100%`
- **爆仓条件** `E ≤ 0`，即价格反向变动达到 `1 / L`

所有「使用」和「获得」的 token 一律 `math.ceil` 向上取整（保证金、名义本金、
卖出所得、爆仓扣款、部分平仓后的剩余仓位金额）；钱包内余额始终以十进制整数存储。

> 报价按「一单位该货币对的市场报价数值」作为指数价格统一处理：
> EURUSD/GBPUSD/XAUUSD 为 USD/单位，USDJPY 使用市场惯用的 JPY/USD 数值。
> 这是模拟盘的简化口径，不是券商成交价，不构成投资建议。

## 报价与爆仓

- 数据源按序回退，全部免 key：`stooq` CSV → `exchangerate.fun` →
  `frankfurter v2 + gold-api` → `open.er-api + goldprice.dev`。
  某个源缺失个别品种时沿用上次成功价；四组都取不到则本次不更新、不做爆仓判定。
- **每次拉取报价后都会检查爆仓**：定时拉取、买入、卖出、查报价触发的拉取都算。
- 按「用户 + 货币对」汇总权益，`E ≤ 0` 时强平该货币对**全部**持仓，
  不足部分 `ceil(-E)` 从钱包账户等额扣除（允许扣成负数）。
- 同一会话（`unified_msg_origin`，即同一个群）内所有用户的爆仓信息合并成一条消息推送：
  ```
  小明持有的EURUSD爆仓了，已强制平仓。
  小红持有的XAUUSD爆仓了，已强制平仓。
  ```
  用户需先在该群用过一次本插件，插件才能记录该群的会话标识。

## 钱包互通要点

- 钱包路径固定为 `data/plugin_data/astrbot_plugin_token_wallet/wallet.json`。
- 每次需要余额时都**重新读文件**，不保留任何 wallet 缓存；读改写全程 `threading.Lock`，
  并用「同目录临时文件 + `os.replace`」原子写回。
- 新用户自动建号：`user_id` / `name`（取自 `event.get_sender_name()`）/ `token: 500000`，
  指令内用 `yield`、LLM 钩子内用 `await event.send()` 发「是新用户咕！已发放新手奖励」，
  并附 `+{_format_token(500000)}`。
- 所有面向用户的 token 数字都经过 `_format_token()`；负余额用带符号版本展示。
- 注册了 `@filter.on_llm_request()`：负余额用户拦截 LLM 请求，并做新用户建号。
  本插件自身**不触发** LLM 调用，因此不注册 `on_llm_response()` 扣费，避免与钱包插件重复扣费。

## 数据文件

- 插件自身状态：`data/plugin_data/astrbot_plugin_fx/fx_state.json`
  （持仓、最新报价、来源与时间、用户会话映射）。持仓数据不写入插件目录，重装不丢。
- 钱包余额：由 `astrbot_plugin_token_wallet` 维护的 `wallet.json`。

## 已知限制

- 免费行情源可能限流、延迟或临时不可用；已做多源回退，但不保证 7×24 实时。
- 同一货币对允许多笔独立持仓，查仓会逐笔输出多行；卖出按货币对对所有笔同比例减仓。
- 爆仓推送需要对应平台支持主动消息（见 AstrBot「消息的发送」文档）。
