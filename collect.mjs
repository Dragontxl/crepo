/**
 * A 股盘中数据采集脚本（GitHub Actions 运行，Node 20 ESM，零依赖）
 *
 * 节拍：8 秒/拍，A/B 两 Agent 相位差 4 秒（PHASE_OFFSET=0/4）
 * 时段（北京时间）：09:25:00-11:30:00、13:00:00-15:00:00
 * 每拍：采集事件型快照（板块异动/涨停/跌停/炸板/情绪指标/人气热榜/晋级）→ POST /api/report
 * 检查点（10:30/11:30/13:30/14:30/15:00）：指数全日序列 → POST /api/report-indices
 * 晋级数据（jinji）：每 120 秒采集一次，两拍之间沿用上次结果
 *
 * 环境变量（GitHub Secrets）：
 *   REPORT_ENDPOINT  上报地址，如 https://ashare-data.ldragon.xyz
 *   SECRET_TOKEN     上报 Bearer Token
 *   PHASE_OFFSET     相位偏移秒数（A=0，B=4）
 *   AGENT_ID         Agent 标识（A / B）
 */

const ENDPOINT = process.env.REPORT_ENDPOINT || '';
const TOKEN = process.env.SECRET_TOKEN || '';
const PHASE_OFFSET = Number(process.env.PHASE_OFFSET || 0) * 1000;
const AGENT_ID = process.env.AGENT_ID || (PHASE_OFFSET === 0 ? 'A' : 'B');

const BEAT_MS = 8000;
const JINJI_INTERVAL_MS = 120_000;
const HEADERS = {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
};

// ---------- 时间工具（北京时间 UTC+8） ----------

function beijingNow() {
  return new Date(Date.now() + 8 * 3600 * 1000);
}

function beijingDateStr(d = beijingNow()) {
  return d.toISOString().slice(0, 10);
}

function hhmm(d = beijingNow()) {
  return d.toISOString().slice(11, 16);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function withRetry(fn, maxRetries = 2, delay = 800) {
  return (async () => {
    let lastErr;
    for (let i = 0; i <= maxRetries; i++) {
      try {
        return await fn();
      } catch (e) {
        lastErr = e;
        if (i < maxRetries) await sleep(delay * (i + 1));
      }
    }
    throw lastErr;
  })();
}

// ---------- 抓取函数（与 ashare-data/src/sync 同源） ----------

async function fetchPool(poolName) {
  const resp = await withRetry(() =>
    fetch(`https://flash-api.xuangubao.cn/api/pool/detail?pool_name=${poolName}`, {
      headers: { ...HEADERS, Referer: 'https://xuangubao.cn/' },
      signal: AbortSignal.timeout(10000),
    })
  );
  const data = await resp.json();
  return data.data || [];
}

async function fetchSectorEvents() {
  try {
    const resp = await fetch('https://api.xuangubao.cn/api/messages/todayDaPanYiDong?headmark=0', {
      headers: { ...HEADERS, Referer: 'https://xuangubao.cn/' },
      signal: AbortSignal.timeout(10000),
    });
    const data = await resp.json();
    const events = data.Messages || data.messages || [];
    return {
      time: events.map((e) => tsSec(e.CreatedAt || e.createdAt || e.timestamp)),
      event: events.map((e) => (e.Title || e.title || '').split('，')[0]),
      clue: events.map((e) => e.Title || e.title || ''),
    };
  } catch (e) {
    console.error('板块异动失败:', e.message);
    return { time: [], event: [], clue: [] };
  }
}

// 事件时间戳统一为秒（防御毫秒）；0 视为无效
function tsSec(v) {
  if (typeof v !== 'number' || !v) return 0;
  return v > 1e12 ? v / 1000 : v;
}

// 三池事件流与 V12 后端直连逻辑对齐（fetchLimitUpData / fetchLimitDownData / fetchLimitUpBrokenData）：
//  - 时间字段：涨停取 last_limit_up、跌停取 last_limit_down、炸板取 last_break_limit_up；时间戳为 0 的条目剔除
//  - 标签：仅用股票名（炸板/跌停次数附加在逗号之后，前端取逗号前作为竖排标签），
//    不再拼接 (代码)——竖排标签若含代码会导致单个标签高度约为 4 倍，相邻时间点大量重叠错乱
//  - 线索（点击明细）：与 V12 一致（连板数/板块名/涨停原因）
function poolToEvents(items, mode) {
  const cfg = {
    up:     { timeField: 'last_limit_up',         breakField: 'break_limit_up_times',   breakThreshold: 0 },
    down:   { timeField: 'last_limit_down',       breakField: 'break_limit_down_times', breakThreshold: 1 },
    broken: { timeField: 'last_break_limit_up',   breakField: 'break_limit_up_times',   breakThreshold: 1 },
  }[mode];

  const rows = items.filter((i) => tsSec(i[cfg.timeField]) > 0);

  return {
    time: rows.map((i) => tsSec(i[cfg.timeField])),
    event: rows.map((i) => {
      const name = String(i.stock_chi_name || '').replace(/\s/g, '');
      const breakTimes = i[cfg.breakField] || 0;
      return breakTimes > cfg.breakThreshold ? `${name},${breakTimes}` : name;
    }),
    clue: rows.map((i) => {
      const sr = i.surge_reason;
      if (mode === 'down') {
        return typeof sr === 'string' ? sr : '';
      }
      let reason = '';
      if (sr && typeof sr === 'object') {
        const plate = sr.related_plates && sr.related_plates[0];
        if (plate) {
          reason = mode === 'broken' ? (plate.plate_name || '') : `${plate.plate_name || ''},${plate.plate_reason || ''}`;
        }
        if (sr.stock_reason) reason += (reason ? (mode === 'broken' ? '，' : '。') : '') + sr.stock_reason;
      }
      if (mode === 'up' && i.limit_up_days) reason = `${i.limit_up_days}板,${reason}`;
      return reason;
    }),
  };
}

function createStockObject(item) {
  return {
    symbol: item.symbol,
    stock_chi_name: item.stock_chi_name,
    change_percent: item.change_percent,
    price: item.price,
    turnover_ratio: item.turnover_ratio,
    non_restricted_capital: item.non_restricted_capital,
    total_capital: item.total_capital,
    first_limit_up: item.first_limit_up,
    last_limit_up: item.last_limit_up,
    last_break_limit_up: item.last_break_limit_up,
    break_limit_up_times: item.break_limit_up_times,
    limit_up_days: item.limit_up_days,
    limit_down_days: item.limit_down_days,
    m_days_n_boards_boards: item.m_days_n_boards_boards,
    m_days_n_boards_days: item.m_days_n_boards_days,
    surge_reason: item.surge_reason || null,
    first_limit_down: item.first_limit_down,
    last_limit_down: item.last_limit_down,
    break_limit_down_times: item.break_limit_down_times,
    first_break_limit_up: item.first_break_limit_up,
    first_break_limit_down: item.first_break_limit_down,
    last_break_limit_down: item.last_break_limit_down,
    is_new_stock: item.is_new_stock,
    listed_date: item.listed_date,
    issue_price: item.issue_price,
    stock_type: item.stock_type,
    limit_timeline: item.limit_timeline,
    mtm: item.mtm,
    buy_lock_volume_ratio: item.buy_lock_volume_ratio,
    sell_lock_volume_ratio: item.sell_lock_volume_ratio,
    volume_bias_ratio: item.volume_bias_ratio,
    nearly_new_acc_pcp: item.nearly_new_acc_pcp,
    nearly_new_break_days: item.nearly_new_break_days,
    new_stock_acc_pcp: item.new_stock_acc_pcp,
    new_stock_break_limit_up: item.new_stock_break_limit_up,
    new_stock_limit_up_days: item.new_stock_limit_up_days,
    new_stock_limit_up_price_before_broken: item.new_stock_limit_up_price_before_broken,
    yesterday_break_limit_up_times: item.yesterday_break_limit_up_times,
    yesterday_first_limit_up: item.yesterday_first_limit_up,
    yesterday_last_limit_up: item.yesterday_last_limit_up,
    yesterday_limit_down_days: item.yesterday_limit_down_days,
    yesterday_limit_up_days: item.yesterday_limit_up_days,
  };
}

function mergeReviewStocks(limitUp, limitDown, broken) {
  const map = new Map();
  for (const item of limitUp) map.set(item.symbol, createStockObject(item));
  for (const item of limitDown) {
    if (map.has(item.symbol)) {
      const s = map.get(item.symbol);
      s.limit_down_days = item.limit_down_days;
      s.first_limit_down = item.first_limit_down;
      s.last_limit_down = item.last_limit_down;
      s.break_limit_down_times = item.break_limit_down_times;
      s.first_break_limit_down = item.first_break_limit_down;
      s.last_break_limit_down = item.last_break_limit_down;
    } else {
      map.set(item.symbol, createStockObject(item));
    }
  }
  for (const item of broken) {
    if (map.has(item.symbol)) {
      const s = map.get(item.symbol);
      s.break_limit_up_times = item.break_limit_up_times;
      s.last_break_limit_up = item.last_break_limit_up;
      s.first_break_limit_up = item.first_break_limit_up;
    } else {
      map.set(item.symbol, createStockObject(item));
    }
  }
  return [...map.values()];
}

async function fetchIndicators() {
  try {
    const resp = await withRetry(() =>
      fetch(
        'https://flash-api.xuangubao.com.cn/api/market_indicator/line?fields=rise_count,fall_count,limit_up_count,limit_down_count,limit_up_broken_count,limit_up_broken_ratio,yesterday_limit_up_avg_pcp',
        { headers: { ...HEADERS, Referer: 'https://xuangubao.cn/' }, signal: AbortSignal.timeout(10000) }
      )
    );
    const data = await resp.json();
    const latest = Array.isArray(data.data) && data.data.length > 0 ? data.data[data.data.length - 1] : data.data || {};
    return {
      rise_count: latest.rise_count || 0,
      fall_count: latest.fall_count || 0,
      limit_up_count: latest.limit_up_count || 0,
      limit_down_count: latest.limit_down_count || 0,
      limit_up_broken_count: latest.limit_up_broken_count || 0,
      limit_up_broken_ratio: latest.limit_up_broken_ratio || 0,
      yesterday_limit_up_avg_pcp: latest.yesterday_limit_up_avg_pcp || 0,
    };
  } catch (e) {
    console.error('情绪指标失败:', e.message);
    return {};
  }
}

async function fetchTurnover() {
  try {
    const resp = await withRetry(() =>
      fetch(
        'https://x-quote.cls.cn/v2/quote/a/stock/emotion?app=CailianpressWeb&os=web&sv=8.4.6&sign=9f8797a1f4de66c2370f7a03990d2737',
        { headers: { ...HEADERS, Referer: 'https://www.cls.cn/' }, signal: AbortSignal.timeout(10000) }
      )
    );
    const data = await resp.json();
    // 数据为空/异常时返回 null（而非假值 '0'），Go/前端对空值显示 '--'
    return {
      turnover: data.data?.turnover ?? null,
      turnover_change: data.data?.turnover_change ?? null,
    };
  } catch (e) {
    console.error('成交额失败:', e.message);
    return {};
  }
}

async function fetchThreeIndices() {
  try {
    const resp = await fetch('https://qt.gtimg.cn/q=sh000001,sz399001,sz399006', {
      headers: HEADERS,
      signal: AbortSignal.timeout(10000),
    });
    const text = await resp.text();
    const parse = (code) => {
      const m = text.match(new RegExp(`v_${code}="([^"]+)"`));
      if (!m) return {};
      const p = m[1].split('~');
      return { name: p[1], code: p[2], price: parseFloat(p[3]), change: parseFloat(p[31] || 0) };
    };
    return { sz: parse('sh000001'), sc: parse('sz399001'), cyb: parse('sz399006') };
  } catch (e) {
    console.error('三指数失败:', e.message);
    return {};
  }
}

async function fetchHotStocks() {
  try {
    const resp = await withRetry(() =>
      fetch(
        'https://dq.10jqka.com.cn/fuyao/hot_list_data/out/hot_list/v1/stock?stock_type=a&type=hour&list_type=normal',
        {
          headers: { ...HEADERS, Accept: 'application/json, text/plain, */*', Referer: 'https://dq.10jqka.com.cn/', Origin: 'https://dq.10jqka.com.cn' },
          signal: AbortSignal.timeout(10000),
        }
      )
    );
    const data = await resp.json();
    if (data.status_code !== 0 || !data.data) return [];
    return (data.data.stock_list || []).slice(0, 15).map((s) => ({
      order: s.order,
      code: s.code,
      name: s.name,
      rise_percent: typeof s.rise_and_fall === 'number' ? s.rise_and_fall.toFixed(2) + '%' : 'N/A',
      rate: s.rate,
      concept_tag: (s.tag?.concept_tag || []).slice(0, 3).join(', '),
      popularity_tag: s.tag?.popularity_tag || '',
      analyse_title: s.tag?.analyse_title || '',
    }));
  } catch (e) {
    console.error('人气热榜失败:', e.message);
    return [];
  }
}

async function fetchJinji() {
  try {
    const resp = await fetch('https://duanxianxia.com/vendor/stockdata/jinjidata.json', {
      headers: { ...HEADERS, Referer: 'https://duanxianxia.com/' },
      signal: AbortSignal.timeout(15000),
    });
    const data = await resp.json();
    return { date: data.date || beijingDateStr(), html: data.html || '', fetched_ts: Math.floor(Date.now() / 1000) };
  } catch (e) {
    console.error('晋级数据失败:', e.message);
    return null;
  }
}

// ---------- 指数检查点 ----------

async function fetchMarketTrend() {
  try {
    const resp = await withRetry(() =>
      fetch('https://api-ddc-wscn.xuangubao.cn/market/trend?fields=tick_at,close_px&prod_code=000001.SS', {
        headers: { ...HEADERS, Referer: 'https://xuangubao.cn/' },
        signal: AbortSignal.timeout(10000),
      })
    );
    const data = await resp.json();
    return data.data || {};
  } catch (e) {
    console.error('上证分时失败:', e.message);
    return {};
  }
}

async function fetchCurrentIndex() {
  try {
    const resp = await withRetry(() =>
      fetch('https://qt.gtimg.cn/q=sh000001', { headers: HEADERS, signal: AbortSignal.timeout(10000) })
    );
    const text = await resp.text();
    const m = text.match(/v_sh000001="([^"]+)"/);
    if (!m) return {};
    const p = m[1].split('~');
    return { name: p[1], code: p[2], price: parseFloat(p[3]), change: parseFloat(p[4]) };
  } catch (e) {
    console.error('当前指数失败:', e.message);
    return {};
  }
}

async function fetchPrevCloseTHSAllA() {
  try {
    const resp = await fetch('https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr/quote/v1/single_kline', {
      method: 'POST',
      headers: { ...HEADERS, 'Content-Type': 'application/json', Referer: 'https://www.iwencai.com/', Origin: 'https://www.iwencai.com', 'x-auth-appname': 'AINVEST', 'x-auth-type': 'ths' },
      body: JSON.stringify({ code_list: [{ codes: ['883421'], market: '48' }], trade_class: 'intraday', time_period: 'day_1', adjust_type: 'forward', begin_time: -10, end_time: 0, trade_date: -1, gpid: 1 }),
      signal: AbortSignal.timeout(10000),
    });
    const result = await resp.json();
    const values = result.data?.quote_data?.[0]?.value || [];
    if (values.length < 2) return 0;
    return values[values.length - 2][4] || 0;
  } catch (e) {
    return 0;
  }
}

async function fetchTHSAllA(dateStr) {
  try {
    const d = new Date(dateStr + 'T00:00:00Z');
    const startTime = new Date(d.getTime() + (9 * 60 + 30) * 60000 - 8 * 3600000).getTime();
    const endTime = new Date(d.getTime() + 15 * 60 * 60000 - 8 * 3600000).getTime();
    const [trendResp, prevClose] = await Promise.all([
      fetch('https://quota-h.10jqka.com.cn/fuyao/common_hq_aggr/quote/v1/single_trend', {
        method: 'POST',
        headers: { ...HEADERS, 'Content-Type': 'application/json', Referer: 'https://www.iwencai.com/', Origin: 'https://www.iwencai.com', 'x-auth-appname': 'AINVEST', 'x-auth-type': 'ths' },
        body: JSON.stringify({ code_list: [{ codes: ['883421'], market: '48' }], begin_time: startTime, end_time: endTime, gpid: 1, time_zone: 'Asia/Shanghai', trade_class: 'intraday', data_fields: ['1', '10', '13', '19'] }),
        signal: AbortSignal.timeout(10000),
      }),
      fetchPrevCloseTHSAllA(),
    ]);
    const result = await trendResp.json();
    const quote = result.data?.quote_data?.[0];
    if (!quote) return {};
    const fields = quote.data_fields || [];
    const trendData = (quote.value || []).map((item) => {
      const point = {};
      fields.forEach((f, i) => (point[f] = item[i]));
      return { timestamp: point['1'] || 0, price: point['10'] || 0, change: point['13'] || 0, volume: point['19'] || 0 };
    });
    return { code: '883421', name: '同花顺全A', trend_data: trendData, date: dateStr, prev_close: prevClose };
  } catch (e) {
    console.error('同花顺全A失败:', e.message);
    return {};
  }
}

// ---------- 上报 ----------

async function postJson(path, body) {
  let lastErr;
  // 最多 3 次尝试；只对网络错误（fetch failed / 超时）重试，HTTP 4xx/5xx 直接抛出
  for (let attempt = 0; attempt <= 2; attempt++) {
    try {
      const resp = await fetch(`${ENDPOINT}${path}`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${TOKEN}` },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(25000),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      return resp.json();
    } catch (e) {
      lastErr = e;
      const isHttpError = /^HTTP \d+$/.test(e.message);
      if (isHttpError || attempt >= 2) break;
      await sleep(1000 * (attempt + 1));
    }
  }
  throw lastErr;
}

// ---------- 采集状态（字段级容错：失败沿用上拍值） ----------

const last = {
  events: null,
  indicators: null,
  review: null,
  hot: null,
  jinji: null,
  jinjiFetchedAt: 0,
};

async function collectBeat(beatTs) {
  const date = beijingDateStr();

  // 三池一次采集，事件流与异动股票共用
  const [upItems, downItems, brokenItems] = await Promise.all([
    fetchPool('limit_up'),
    fetchPool('limit_down'),
    fetchPool('limit_up_broken'),
  ]);

  const [sectorEvents, indicatorsRaw, turnover, threeIndices, hotStocks] = await Promise.all([
    fetchSectorEvents(),
    fetchIndicators(),
    fetchTurnover(),
    fetchThreeIndices(),
    fetchHotStocks(),
  ]);

  last.events = {
    板块异动: sectorEvents,
    个股涨停: poolToEvents(upItems, 'up'),
    个股跌停: poolToEvents(downItems, 'down'),
    炸板个股: poolToEvents(brokenItems, 'broken'),
    timestamp: new Date().toISOString(),
  };

  last.indicators = {
    ...indicatorsRaw,
    turnover: turnover.turnover || indicatorsRaw.turnover,
    turnover_change: turnover.turnover_change || indicatorsRaw.turnover_change,
    sz_index_price: threeIndices.sz?.price || indicatorsRaw.sz_index_price,
    sz_index_change: threeIndices.sz?.change || indicatorsRaw.sz_index_change,
    sc_index_price: threeIndices.sc?.price || indicatorsRaw.sc_index_price,
    sc_index_change: threeIndices.sc?.change || indicatorsRaw.sc_index_change,
    cyb_index_price: threeIndices.cyb?.price || indicatorsRaw.cyb_index_price,
    cyb_index_change: threeIndices.cyb?.change || indicatorsRaw.cyb_index_change,
    timestamp: new Date().toISOString(),
  };

  last.review = mergeReviewStocks(upItems, downItems, brokenItems);
  if (hotStocks.length > 0) last.hot = hotStocks;

  // jinji 低频（120 秒）
  if (beatTs - last.jinjiFetchedAt >= JINJI_INTERVAL_MS || !last.jinji) {
    const jinji = await fetchJinji();
    if (jinji) {
      last.jinji = jinji;
      last.jinjiFetchedAt = beatTs;
    }
  }

  // target_ts 取实际采集时间（而非计划节拍 beatTs），避免连续超时导致 beatTs 落后、
  // 被后端 stale_ts 校验拒绝（HTTP 400）。
  const payload = {
    meta: { agent_id: AGENT_ID, target_ts: Math.floor(Date.now() / 1000), date },
    monitor: { events: last.events, indicators: last.indicators },
    review: { stocks: last.review || [], hot_stocks: last.hot || [] },
    jinji: last.jinji || { date, html: '', fetched_ts: 0 },
  };

  return postJson('/api/report', payload);
}

async function collectIndicesCheckpoint() {
  const date = beijingDateStr();
  console.log(`[${hhmm()}] 指数检查点采集...`);
  const [trend, current, thsAllA] = await Promise.all([
    fetchMarketTrend(),
    fetchCurrentIndex(),
    fetchTHSAllA(date),
  ]);
  await postJson('/api/report-indices', {
    date,
    indices: { sse_trend: trend, current, ths_all_a: thsAllA },
  });
  console.log(`[${hhmm()}] 指数检查点已上报`);
}

// ---------- 节拍状态机 ----------

const SESSIONS = [
  { start: '09:25', end: '11:30' },
  { start: '13:00', end: '15:00' },
];
const CHECKPOINTS = ['10:30', '11:30', '13:30', '14:30', '15:00'];

function sessionBounds(session) {
  const [sh, sm] = session.start.split(':').map(Number);
  const [eh, em] = session.end.split(':').map(Number);
  const now = beijingNow();
  const start = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), sh - 8, sm, 0);
  const end = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), eh - 8, em, 0);
  return { start, end };
}

async function runSession(session) {
  const { start, end } = sessionBounds(session);
  const doneCheckpoints = new Set();

  // 等待到会话开始（含相位偏移 + 抖动）
  const firstBeat = start + PHASE_OFFSET + Math.floor(Math.random() * 600);
  const waitMs = firstBeat - Date.now();
  if (waitMs > 0) {
    console.log(`等待 ${Math.round(waitMs / 1000)}s 到会话开始 ${session.start}（Agent ${AGENT_ID}，相位 ${PHASE_OFFSET / 1000}s）`);
    await sleep(waitMs);
  }

  let beat = firstBeat > Date.now() ? firstBeat : Date.now();
  let beatCount = 0;

  while (Date.now() < end) {
    beatCount++;
    const nowHm = hhmm();
    try {
      const result = await collectBeat(beat);
      if (beatCount % 30 === 0 || result.changed === false) {
        console.log(`[${nowHm}] 拍 ${beatCount} ${result.changed ? '已更新并广播' : '无变化'}`);
      }
    } catch (e) {
      console.error(`[${nowHm}] 拍 ${beatCount} 上报失败:`, e.message);
    }

    // 指数检查点（跨过时间点即触发，每点一次）
    for (const cp of CHECKPOINTS) {
      if (!doneCheckpoints.has(cp) && nowHm >= cp && hhmm() < '15:05') {
        doneCheckpoints.add(cp);
        try {
          await collectIndicesCheckpoint();
        } catch (e) {
          console.error(`检查点 ${cp} 失败:`, e.message);
        }
      }
    }

    beat += BEAT_MS;
    // 节拍一旦落后实际时间立即对齐到当前时间，避免延迟累积导致无间隔追拍。
    if (beat < Date.now()) {
      beat = Date.now();
    }
    const sleepMs = beat - Date.now();
    if (sleepMs > 0) await sleep(sleepMs);
  }
  console.log(`会话 ${session.start}-${session.end} 结束`);
}

async function main() {
  if (!ENDPOINT || !TOKEN) {
    console.error('缺少 REPORT_ENDPOINT / SECRET_TOKEN 环境变量');
    process.exit(1);
  }
  console.log(`采集 Agent ${AGENT_ID} 启动，上报地址 ${ENDPOINT}`);

  // 单次启动只执行「结束时间尚未到达」的第一个会话：
  // 09:20 触发 → 跑早盘(09:25-11:30)；12:55 触发 → 早盘已过，跑午盘(13:00-15:00)。
  // 避免单次运行同时覆盖早盘+午盘，导致 09:20 与 12:55 两次触发在午盘重复采集。
  const now = Date.now();
  const session = SESSIONS.find((s) => sessionBounds(s).end > now);

  if (!session) {
    console.log('当前无待执行的交易会话，退出');
    return;
  }
  console.log(`执行会话 ${session.start}-${session.end}`);
  await runSession(session);
  console.log('采集结束');
}

main().catch((e) => {
  console.error('采集进程异常退出:', e);
  process.exit(1);
});
