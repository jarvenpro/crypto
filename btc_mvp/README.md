# BTC 日内分析助手 MVP

这是一个适合 Windows 小白的 BTC 日内分析机器人。
现在它已经支持两条链路：

- `Binance` 技术面与合约结构
- `CryptoCompare + Gemini` 新闻面与 AI 快读
- `Binance 强平流 + FMP 宏观日历` 风控过滤

它的目标不是做中长期判断，而是只给你一个盘口时段内、最多不超过 12 小时的短线观察。

## 这版已经能做什么

- 获取 `BTCUSDT` 现货 5 分钟 K 线
- 获取盘口买一卖一
- 获取合约侧 `OI / Taker Ratio / Long Short Ratio / Funding`
- 获取 CryptoCompare BTC 相关新闻
- 采样 Binance 强平流，判断短线是空头回补还是多头踩踏
- 获取 FMP 宏观日历，识别高影响事件窗口
- 对新闻做缓存、去重和免费额度保护
- 在有相关新闻时调用 Gemini 生成 3 行短评
- 严格按交易时段控制方向类提醒
- 在场外只保留高优先级风险提醒
- 推送到 Telegram
- 保存历史信号、新闻记录和错误日志

## 当前目录

- `run_bot.py`
  运行入口
- `app/clients/binance.py`
  Binance 数据接口
- `app/clients/cryptocompare.py`
  CryptoCompare 新闻接口
- `app/clients/gemini.py`
  Gemini 接口
- `app/services/analysis.py`
  技术面规则和 Telegram 消息渲染
- `app/services/news.py`
  新闻筛选、去重、缓存、AI 提示词
- `scripts/install_scheduled_task.ps1`
  一键安装 Windows 定时任务
- `data/`
  运行后自动生成的状态和日志

## 第一步：配置 `.env`

打开 `.env`，至少填这 5 项：

```env
TELEGRAM_BOT_TOKEN=你的 Telegram Bot Token
TELEGRAM_CHAT_ID=你的 Telegram Chat ID
CRYPTOCOMPARE_API_KEY=你的 CryptoCompare API Key
GEMINI_API_KEY=你的 Gemini API Key
GEMINI_MODEL=gemini-2.5-flash
FMP_API_KEY=你的 FMP API Key
```

建议保留这几个默认开关：

```env
NEWS_REQUIRE_EXPLICIT_BTC=false
STRICT_SESSION_MODE=true
ALLOW_OUTSIDE_SESSION_RISK_ALERTS=true
MIN_CONFIDENCE_FOR_TRADE=62
MIN_RISK_REWARD=1.8
MIN_COMPOSITE_SCORE_FOR_TRADE=22
MACRO_PRE_BLOCK_MINUTES=20
MACRO_POST_BLOCK_MINUTES=20
```

含义是：

- `NEWS_REQUIRE_EXPLICIT_BTC=false`
  允许宏观、战争、特朗普、ETF、矿工、巨鲸等未直接点名 BTC 的高影响新闻进入筛选
- `STRICT_SESSION_MODE=true`
  不在你设定的交易时段内，不发方向类信号
- `ALLOW_OUTSIDE_SESSION_RISK_ALERTS=true`
  场外如果出现高风险突发消息，允许发“风险提醒”，但不是开仓信号
- `MIN_CONFIDENCE_FOR_TRADE=62`
  只有综合置信度足够高才会给交易计划
- `MIN_RISK_REWARD=1.8`
  只有第一目标盈亏比至少 `1.8` 才推荐出手
- `MIN_COMPOSITE_SCORE_FOR_TRADE=22`
  综合分没达到阈值就继续观望
- `MACRO_PRE_BLOCK_MINUTES=20`
  高影响宏观事件前 20 分钟不建议开新仓
- `MACRO_POST_BLOCK_MINUTES=20`
  高影响宏观事件后 20 分钟也不建议立刻开新仓

## 第二步：测试新闻链路

```powershell
py scripts\test_news_pipeline.py
```

如果你的 `CRYPTOCOMPARE_API_KEY` 和 `GEMINI_API_KEY` 已填好，它会先打印新闻筛选结果，再输出一段 Gemini 短评。

## 第三步：跑增强版机器人

在 `D:\crypto\btc_mvp` 打开 PowerShell，然后执行：

```powershell
py run_bot.py
```

如果当前综合信号够强、盈亏比够好，或者抓到了新的高影响新闻，它就会推送到 Telegram。
如果没达到阈值，它会先把完整预览打印在终端里。

## 第四步：安装 24 小时自动运行

直接执行：

```powershell
powershell -ExecutionPolicy Bypass -File scripts\install_scheduled_task.ps1
```

它会创建一个名为 `BTC_MVP_BOT` 的 Windows 定时任务，每 5 分钟自动运行一次。

## 这次新增的实战规则

- 新闻标题会先去掉媒体名前缀，降低 `Bitcoin World:` 这类误判
- 宏观、战争、特朗普、ETF、矿工、巨鲸等未直接写 BTC 的高影响消息也会纳入
- 会采样 Binance 强平流，帮助你识别是否正在发生空头回补或多头踩踏
- 会识别 CPI、非农、FOMC、Powell、利率决议等宏观事件窗口
- 场外不再推方向单，只保留高优先级风险提醒
- 不再只猜方向，而是先算“这个位置的盈亏比够不够好”
- 止损和止盈会参考支撑阻力、前高前低、Session High/Low 和 ATR

## 建议的下一步

等你观察半天到一天后，我们可以继续做：

- 欧盘 / 美盘分开配置
- 更细的“突破确认”规则
- 宏观事件过滤
- Telegram 命令交互
- 简单复盘统计

## GPT 聚合 API

这次额外加入了一个适合网页版自定义 GPT `Actions` 调用的聚合层，入口文件是 `run_gateway.py`。

它做的不是把所有上游接口原样暴露给 GPT，而是分成两层：

- `overview` 聚合接口
  适合 GPT 直接调用，减少它在几十个接口里乱选的概率
- `sources` 单源接口
  适合你后续排错、扩展或让 GPT 精确补查某个数据源

### 当前聚合的免费 / 免费层数据源

- `Binance`
  实时现货和合约结构
- `Treasury Fiscal Data`
  美国财政部利率数据
- `BLS`
  宏观时间序列
- `FRED`
  常用宏观序列，需要你自己的免费 API Key
- `BEA`
  GDP 等数据，需要你自己的免费 API Key
- `Federal Reserve RSS`
  FOMC / 货币政策新闻流
- `SEC`
  公司和 ETF 申报文件
- `CFTC`
  比特币 COT 持仓
- `mempool.space`
  比特币链上手续费
- `Fear & Greed`
  情绪指数
- `CoinGecko`
  免费 Demo / Pro 价格接口

### 运行方式

先安装依赖：

```powershell
py -m pip install -r requirements-gateway.txt
```

再启动 API：

```powershell
py run_gateway.py
```

启动后可直接打开：

- `http://127.0.0.1:8000/docs`
- `http://127.0.0.1:8000/openapi.json`

如果你要接到自定义 GPT `Actions`，先把 `.env` 里的 `GATEWAY_PUBLIC_BASE_URL` 改成你的公网 `HTTPS` 地址。

然后导出专用 schema：

```powershell
py scripts\export_gpt_action_schema.py
```

它会生成：

- `openapi-gpt-actions.json`

如果你要接到自定义 GPT `Actions`，优先在 GPT 后台选择 `API Key` 认证，并把 header 名设为：

```text
X-API-Key
```

值设为：

```text
你在 GATEWAY_API_TOKEN 里设置的 token
```

这个网关也兼容 `Authorization: Bearer <token>`，但对 GPT Actions 来说，`X-API-Key` 更直接。

### 主要接口

- `GET /v1/crypto/overview`
- `GET /v1/macro/overview`
- `GET /v1/regulatory/overview`
- `GET /v1/sources/binance/market`
- `GET /v1/sources/coingecko/simple-price`
- `GET /v1/sources/fear-greed/latest`
- `GET /v1/sources/mempool/fees`
- `GET /v1/sources/treasury/latest-avg-rates`
- `GET /v1/sources/bls/series/{series_id}`
- `GET /v1/sources/fred/series/{series_id}`
- `GET /v1/sources/bea/datasets`
- `GET /v1/sources/bea/gdp`
- `GET /v1/sources/fed/monetary-feed`
- `GET /v1/sources/sec/company-tickers`
- `GET /v1/sources/sec/submissions/{entity}`
- `GET /v1/sources/cftc/bitcoin-cot`

### 烟雾测试

```powershell
py scripts\test_gateway_smoke.py
```

这个测试会检查服务能否正常生成 `health` 和 `openapi.json`。
