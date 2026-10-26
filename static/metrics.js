'use strict';
const MetricsView = (() => {
  const finite = value => typeof value === 'number' && Number.isFinite(value) && value >= 0;
  const count = value => finite(value) ? value.toLocaleString('zh-TW') : '無資料';
  const duration = value => !finite(value) ? '無資料' : value < 1000 ? `${Math.round(value)} ms` : `${(value / 1000).toFixed(2)} 秒`;
  const percent = value => finite(value) ? `${(value * 100).toFixed(1)}%` : '無資料';
  function sortedAgents(agents, sort) {
    const value = agent => sort === 'latency' ? agent.latency.p95_ms : agent[sort];
    return [...agents].sort((a, b) => (value(b) ?? -1) - (value(a) ?? -1) || a.agent_id.localeCompare(b.agent_id));
  }
  return {count, duration, percent, sortedAgents};
})();
if (typeof module !== 'undefined') module.exports = MetricsView;
if (typeof document !== 'undefined') {
  window.addEventListener('storage', event => {
    if (event.key === 'theme' || event.key === null) {
      document.documentElement.dataset.theme = event.newValue === 'light' ? 'light' : 'dark';
    }
  });
  const $ = id => document.getElementById(id);
  const {count, duration, percent, sortedAgents} = MetricsView;
  const labels = {done:'成功', error:'錯誤', timeout:'逾時', cancelled:'取消', busy:'忙碌', running:'進行中', unknown:'未知'};
  let snapshot = null, controller = null;
  const node = (tag, text, className) => {
    const element = document.createElement(tag);
    if (text !== undefined) element.textContent = text;
    if (className) element.className = className;
    return element;
  };
  function row(target, cells) {
    const tr = node('tr');
    cells.forEach(value => tr.append(node('td', value)));
    target.append(tr);
  }
  function empty(target, columns, text) {
    const td = node('td', text, 'empty');
    td.colSpan = columns;
    const tr = node('tr'); tr.append(td); target.append(tr);
  }
  function renderAgents() {
    const target = $('agents'); target.replaceChildren();
    const agents = sortedAgents(snapshot.agents, $('sort').value);
    agents.forEach(a => row(target, [a.name, count(a.turns), count(a.calls), percent(a.failure_rate),
      duration(a.latency.mean_ms), duration(a.latency.p95_ms) + (a.latency.p95_sampled ? '（抽樣）' : ''), count(a.llm_calls), `${count(a.input_tokens)} / ${count(a.output_tokens)}`]));
    if (!agents.length) empty(target, 8, '這段期間沒有 Agent 紀錄。');
  }
  function render(data) {
    snapshot = data;
    const s = data.summary, cards = $('cards'); cards.replaceChildren();
    const definitions = [
      ['已儲存回答', count(s.turns), `成功 ${count(s.statuses.done ?? 0)} · 取消 ${count(s.statuses.cancelled ?? 0)}`],
      ['回答失敗率', percent(s.failure_rate), `逾時 ${count(s.statuses.timeout ?? 0)} · 錯誤 ${count(s.statuses.error ?? 0)}`],
      ['回答 P95 耗時', duration(s.latency.p95_ms), `平均 ${duration(s.latency.mean_ms)}${s.latency.p95_sampled ? ' · P95 為抽樣估計' : ''}`],
      ['平均累計排隊', duration(s.queue.mean_ms), `P95 ${duration(s.queue.p95_ms)}`],
      ['已回報 token', `${count(s.reported_usage.input_tokens)} / ${count(s.reported_usage.output_tokens)}`, '輸入 / 輸出 · 部分回報可能低估用量']
    ];
    definitions.forEach(([label, value, detail]) => {
      const card = node('article', undefined, 'card');
      card.append(node('div', label, 'label'), node('strong', value), node('small', detail)); cards.append(card);
    });
    $('coverage').textContent = `耗時紀錄 ${count(s.latency.count)} / ${count(s.turns)} 筆；完整用量回報 ${count(s.complete_usage_turns)} / ${count(s.turns)} 筆。${s.turns_without_metrics ? ` ${count(s.turns_without_metrics)} 筆缺少統計資料。` : ''}${s.invalid_metrics ? ` 其中 ${count(s.invalid_metrics)} 筆資料格式不符。` : ''}${data.truncated ? ` 為控制查詢負擔，目前只分析最近 ${count(data.analyzed_turns)} 筆（期間內共 ${count(data.matched_turns)} 筆）；本頁所有指標及排行皆以此範圍計算。` : ''}`;
    if (s.partial_trace_turns) $('coverage').textContent += ` ${count(s.partial_trace_turns)} 筆呼叫追蹤曾截斷，Agent 呼叫數與明細可能不完整。`;
    const daily = $('daily'); daily.replaceChildren();
    const max = Math.max(1, ...data.daily.map(d => d.turns));
    data.daily.forEach(d => {
      const day = node('div', undefined, 'day');
      day.title = `${d.date}：${d.turns} 筆，平均累計排隊 ${duration(d.queue.mean_ms)}`;
      const bar = node('div', undefined, 'day-bar'); bar.style.height = `${Math.max(3, d.turns / max * 105)}px`;
      day.append(node('b', count(d.turns)), bar, node('div', d.date === 'unknown' ? '日期未知' : d.date.slice(5)));
      daily.append(day);
    });
    if (!data.daily.length) daily.append(node('p', '這段期間還沒有已儲存的回答。', 'empty'));
    renderAgents();
    const slow = $('slow'); slow.replaceChildren();
    const names = new Map(data.agents.map(a => [a.agent_id, a.name]));
    data.slow_turns.forEach(t => row(slow, [new Date(t.at).toLocaleString('zh-TW', {timeZone:'Asia/Taipei', hour12:false}),
      names.get(t.agent_id) ?? t.agent_id, labels[t.status] ?? '未知', duration(t.total_ms), duration(t.queue_ms)]));
    if (!data.slow_turns.length) empty(slow, 5, '還沒有可比較耗時的回答。');
    $('content').hidden = false;
  }
  async function load() {
    controller?.abort();
    const active = new AbortController(); controller = active;
    $('refresh').disabled = true; $('notice').className = '';
    $('notice').textContent = '正在載入統計…'; $('content').hidden = true;
    const timeout = setTimeout(() => active.abort(), 20000);
    try {
      const response = await fetch(`/hub/metrics?days=${encodeURIComponent($('days').value)}`, {credentials:'same-origin', signal:active.signal});
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json();
      if (controller !== active) return;
      render(data);
      $('notice').textContent = `已更新 ${new Date().toLocaleTimeString('zh-TW', {hour12:false})}${data.truncated ? ' · 目前顯示部分紀錄，詳見下方說明' : ''}`;
    } catch (error) {
      if (controller !== active) return;
      $('notice').className = 'error';
      $('notice').textContent = `無法載入統計${error.name === 'AbortError' ? '：查詢逾時' : ''}，請稍後按「重新整理」。`;
    } finally {
      clearTimeout(timeout);
      if (controller === active) $('refresh').disabled = false;
    }
  }
  $('refresh').addEventListener('click', load);
  $('days').addEventListener('change', load);
  $('sort').addEventListener('change', () => { if (snapshot) renderAgents(); });
  load();
}
