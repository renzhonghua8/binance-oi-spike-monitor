# Binance USDT Futures OI Spike Monitor

一个用于监控 Binance U 本位永续合约 Open Interest 在 1m、3m、5m 内突然增长的实时页面。它使用 Binance 官方公开 API，不需要 API Key。

## 功能

- 默认监控 Binance Futures 全部 USDT 永续合约
- 可切换为只监控 24h 成交额最高的高流动性合约
- 计算 1m、3m、5m OI 变化率
- 结合 5m 价格变化、5m 成交量倍数、资金费率、24h 成交额生成信号
- 默认突出 `5m OI 增长 > 3%` 且 `成交量放大 > 1.5x` 的标的
- 记录强信号，并统计同一币种 1 小时内重复触发次数
- 区分：多头增仓、空头增仓、挤空、挤多、仅OI增长
- 支持阈值配置、搜索、排序、自动刷新
- 后端通过 SSE 向页面实时推送数据
- 显示每行数据年龄，强信号默认只使用新鲜 OI 数据
- 强信号触发后推送到钉钉群机器人，消息包含关键词“异动”

## 启动

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

需要 Python 3.10 或更高版本。CentOS 默认 Python 版本较旧时，推荐使用 Docker 部署。

## Docker 部署

```bash
git clone https://github.com/renzhonghua8/binance-oi-spike-monitor.git
cd binance-oi-spike-monitor
docker build -t binance-oi-spike-monitor .
docker run -d \
  --name binance-oi-spike-monitor \
  --restart unless-stopped \
  -p 8000:8000 \
  -e DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=你的access_token" \
  binance-oi-spike-monitor
```

查看日志：

```bash
docker logs -f binance-oi-spike-monitor
```

停止服务：

```bash
docker stop binance-oi-spike-monitor
docker rm binance-oi-spike-monitor
```

也可以通过环境变量覆盖钉钉机器人地址：

```bash
export DINGTALK_WEBHOOK="https://oapi.dingtalk.com/robot/send?access_token=你的access_token"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

打开：

```text
http://127.0.0.1:8000
```

## 配置

页面顶部可以调整：

- 监控数量：按 24h USDT 成交额从高到低选取
- 全部合约：开启后扫描全部 USDT 永续合约，监控数量设置会暂时不生效
- OI 5m 阈值
- 5m 成交量倍数阈值
- 强信号分数：默认 50 分，达到后会写入“强信号记录”
- 最大数据年龄：超过后该行会弱化显示，并不会触发强信号
- 启动回填数量：默认只对 24h 成交额靠前的 160 个合约回填历史样本，避免全市场启动太慢
- 最低 24h 成交额
- 刷新间隔

## 实时性方案

全量合约监控不能简单理解为“527 个币同一秒全部刷新”。Binance 当前公开 OI 接口仍需要按 symbol 请求，所以本工具使用更稳的分层方案：

- 价格、24h成交额：通过 Binance 全市场 ticker 批量获取
- 资金费率：通过 premium index 批量获取
- 当前 OI：按 symbol 并发分片扫描
- 启动回填：首次扫描时会对高成交额合约用 1m K 线回填价格/成交额样本，并用 5m OI 历史接口回填 OI 样本
- 1m/3m OI 变化：Binance OI 历史最小周期偏粗，仍以运行后的实时采样为准
- 5m OI 变化：优先使用启动回填后的 OI 样本计算
- 5m价格变化：优先使用启动回填后的价格样本计算
- 5m量倍数：优先使用启动回填后的成交额样本估算
- 数据年龄：每行显示距离最近一次 OI 更新过去了多少秒

如果你追求更强实时性，优先降低刷新秒数到 30-45 秒，并把最大数据年龄设为 60-90 秒。若出现 Binance 限频或超时，再调高刷新秒数。

修改后点击保存，后端会使用新配置重新筛选和计算。

## 信号逻辑

- 多头增仓：OI 增长，价格上涨
- 空头增仓：OI 增长，价格下跌
- 挤空：OI 增长较强，价格快速上涨，资金费率偏正
- 挤多：OI 增长较强，价格快速下跌，资金费率偏负
- 仅OI增长：OI 增长明显，但价格方向不强

信号强度会综合 OI 5m 增长、成交量倍数、价格变化幅度和 24h 成交额计算，范围为 0-100。

## 如何看重复触发

同一币种在 1 小时内多次触发强信号，通常说明市场正在持续关注它，但不代表一定会继续上涨或下跌。

- OI 连续增加、价格沿同一方向推进、成交量持续放大：更像趋势延续
- OI 连续增加、价格高位横盘或快速冲高回落：可能是诱多或多空换手
- OI 增加、价格下跌且反弹弱：更偏空头压制
- OI 突然下降、价格快速反向：可能是平仓或挤压结束

比较稳的使用方式是：先用“强信号记录”找到反复出现的币，再去 Binance 看 1m/5m K 线确认是否突破关键位置，并设置止损。

## 钉钉告警

当某个币触发强信号并写入“强信号记录”时，后端会同步推送到钉钉群。默认 3 分钟内同一币种不会重复记录和重复推送。

默认强信号条件：

- 5m OI 增长达到页面配置阈值
- 5m量倍数达到页面配置阈值
- 强度达到页面配置阈值
- 数据未过期

钉钉机器人设置了关键词时，关键词需要出现在标题或正文里。本项目推送标题包含“异动”。

## 注意

- 1m、3m、5m OI 变化依赖本地运行时滚动采样。刚启动时会使用已采集到的最早样本估算，运行 5 分钟后完整。
- 全部合约模式请求量更大，默认刷新间隔为 45 秒。如果出现限频或网络不稳，可以调高刷新秒数，或者关闭全部合约改用高流动性模式。
- Binance 公共接口在部分网络或地区可能不可用；页面右上角会显示后端拉取状态。
- 这是交易机会监控工具，不构成投资建议。
