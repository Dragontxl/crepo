# crawl-repo（盘中数据采集 Agent）

公开 GitHub 仓库。两个 workflow（A/B 双 Agent，相位 0s/4s）在交易日盘中每 8 秒采集一次，
上报到 ashare-data Worker，由服务端判变写入 R2 并 WebSocket 广播。

## 配置（GitHub → Settings → Secrets and variables → Actions）

| Secret | 说明 |
|---|---|
| `REPORT_ENDPOINT` | Worker 地址，如 `https://ashare-data.ldragon.xyz` |
| `SECRET_TOKEN` | 上报 Bearer Token（与 Worker 的 `REPORT_TOKEN` Secret 一致） |

## 运行

- 定时触发：**cron-job.org** 外部调度（北京时间 09:20 / 12:55，周一至五），POST GitHub workflow_dispatch API 启动；脚本内自行对齐到 09:25 / 13:00 开拍。不用 GitHub 自带 schedule（其触发延迟可达 5-15 分钟且不可控）
- 手动触发：Actions → crawl-A / crawl-B → Run workflow
- 单次运行覆盖全天（09:25-15:00，含午间等待），在 6 小时作业上限内

## 采集内容

- 每拍：板块异动、涨停/跌停/炸板池（事件流 + 异动股票共用）、情绪指标、成交额、三指数、人气热榜
- 晋级数据（jinji）每 120 秒采集一次，期间沿用上次数值
- 检查点（10:30/11:30/13:30/14:30/15:00）：上证分时、腾讯指数、同花顺全A 分时（供收盘归档）
