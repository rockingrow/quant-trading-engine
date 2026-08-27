/* The dashboard: builds the whole page from the embedded view object.
 *
 * Charts are hand-drawn SVG rather than a charting library, for the reason the
 * renderer inlines everything else — the file has to open with no network and
 * no build step, and a vendored library is 200 KB in every report forever. What
 * is here is only what these panels need: linear and time scales, nice ticks, a
 * crosshair, and a tooltip.
 *
 * Every chart re-draws on resize and reads its colours from CSS custom
 * properties, so a theme switch repaints without a redraw.
 */
"use strict";

const VIEW = JSON.parse(document.getElementById("view").textContent);

/* ── Formatting ───────────────────────────────────────────────────────
 *
 * Money is grouped and fixed to two decimals; ratios to two or three; R to two
 * with an explicit sign, because the sign is the whole point of an R-multiple.
 * `null` renders as "n/a" everywhere and never as 0 — a statistic that could
 * not be computed and one that came out zero are different claims.
 */

const NA = "n/a";
const nf = (digits) =>
  new Intl.NumberFormat(undefined, { minimumFractionDigits: digits, maximumFractionDigits: digits });
const F2 = nf(2);
const F0 = nf(0);

function num(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  return nf(digits).format(value);
}
function money(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  return (value > 0 ? "+" : "") + nf(digits).format(value);
}
function pct(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  return nf(digits).format(value) + "%";
}
function signedPct(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  return (value > 0 ? "+" : "") + nf(digits).format(value) + "%";
}
function rMultiple(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  return (value > 0 ? "+" : "") + F2.format(value) + "R";
}
function price(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  const abs = Math.abs(value);
  return nf(abs >= 1000 ? 2 : abs >= 100 ? 3 : abs >= 1 ? 4 : 6).format(value);
}
function compact(value) {
  if (value === null || value === undefined || Number.isNaN(value)) return NA;
  const abs = Math.abs(value);
  if (abs >= 1e9) return (value / 1e9).toFixed(2) + "B";
  if (abs >= 1e6) return (value / 1e6).toFixed(2) + "M";
  if (abs >= 1e4) return (value / 1e3).toFixed(1) + "k";
  return F2.format(value);
}
function byKind(value, kind) {
  switch (kind) {
    case "currency": return money(value);
    case "percent": return pct(value);
    case "ratio": return num(value, 3);
    case "r": return rMultiple(value);
    case "count": return value === null ? NA : num(value, Number.isInteger(value) ? 0 : 1);
    default: return num(value);
  }
}
const ms = (iso) => (iso ? Date.parse(iso) : NaN);
function dateLabel(iso, withTime) {
  const at = new Date(ms(iso));
  if (Number.isNaN(at.getTime())) return NA;
  const date = at.toISOString().slice(0, 10);
  return withTime ? date + " " + at.toISOString().slice(11, 16) : date;
}
function tone(value) {
  return value > 0 ? "up" : value < 0 ? "down" : "";
}

/* ── DOM helpers ─────────────────────────────────────────────────────── */

function h(tag, attrs, ...kids) {
  const node = document.createElement(tag);
  apply(node, attrs);
  add(node, kids);
  return node;
}
function svgEl(tag, attrs, ...kids) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  apply(node, attrs);
  add(node, kids);
  return node;
}
function apply(node, attrs) {
  for (const [key, value] of Object.entries(attrs || {})) {
    if (value === null || value === undefined || value === false) continue;
    // Text only, never markup: strategy names, params and diagnostic evidence
    // all come from the report, and there is no reason for any of them to be
    // parsed as HTML. Without an innerHTML path there is nothing to get wrong.
    if (key === "text") node.textContent = value;
    else if (key === "class") node.setAttribute("class", value);
    else if (key.startsWith("on")) node.addEventListener(key.slice(2), value);
    else node.setAttribute(key, value);
  }
}
function add(node, kids) {
  for (const kid of kids.flat()) {
    if (kid === null || kid === undefined || kid === false) continue;
    node.appendChild(typeof kid === "string" ? document.createTextNode(kid) : kid);
  }
}

/* ── Scales, ticks, paths ────────────────────────────────────────────── */

function scale(d0, d1, r0, r1) {
  const span = d1 - d0 || 1;
  return (value) => r0 + ((value - d0) * (r1 - r0)) / span;
}

function niceTicks(min, max, count = 5) {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return [0];
  if (min === max) return [min];
  const raw = (max - min) / count;
  const magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1, 2, 2.5, 5, 10].map((f) => f * magnitude).find((f) => f >= raw) || magnitude * 10;
  const first = Math.ceil(min / step) * step;
  const out = [];
  for (let value = first; value <= max + step * 1e-6; value += step) out.push(Number(value.toFixed(10)));
  return out.length ? out : [min, max];
}

/** Pad a numeric domain so the extremes are not drawn on the frame itself. */
function padDomain(min, max, slack = 0.08) {
  if (!Number.isFinite(min) || !Number.isFinite(max)) return [0, 1];
  if (min === max) return min === 0 ? [-1, 1] : [min - Math.abs(min) * 0.1, max + Math.abs(max) * 0.1];
  const pad = (max - min) * slack;
  return [min - pad, max + pad];
}

const linePath = (points) =>
  points.map((point, index) => (index ? "L" : "M") + point[0].toFixed(2) + " " + point[1].toFixed(2)).join(" ");

function timeTicks(from, to, count) {
  const span = to - from;
  if (!Number.isFinite(span) || span <= 0) return [from];
  const out = [];
  for (let index = 0; index <= count; index++) out.push(from + (span * index) / count);
  return out;
}
function timeFormatter(span) {
  const day = 86400000;
  if (span > 400 * day) return (value) => new Date(value).toISOString().slice(0, 7);
  if (span > 3 * day) return (value) => new Date(value).toISOString().slice(5, 10);
  return (value) => new Date(value).toISOString().slice(11, 16);
}

/**
 * A vertical gradient that switches colour exactly at the zero line.
 *
 * An equity area painted one colour is misleading in the stretch where the
 * strategy was down: the eye reads a green fill below zero as profit. Splitting
 * the paint at zero costs one gradient and removes the ambiguity. The stops are
 * inline styles rather than attributes so they resolve CSS variables, and a
 * theme switch repaints them with everything else.
 */
let gradientSeq = 0;
function signGradient(svg, plot, y, lo, hi) {
  if (!(lo < 0 && hi > 0)) return null;
  const id = "sign-" + ++gradientSeq;
  const at = Math.min(1, Math.max(0, (y(0) - plot.y0) / (plot.y1 - plot.y0 || 1)));
  const stops = [
    [0, "var(--up)"],
    [at, "var(--up)"],
    [at, "var(--down)"],
    [1, "var(--down)"],
  ].map(([offset, colour]) => svgEl("stop", { offset, style: `stop-color:${colour}` }));
  const gradient = svgEl(
    "linearGradient",
    { id, x1: 0, x2: 0, y1: plot.y0, y2: plot.y1, gradientUnits: "userSpaceOnUse" },
    ...stops
  );
  svg.appendChild(svgEl("defs", {}, gradient));
  return `url(#${id})`;
}

/* ── The tooltip ─────────────────────────────────────────────────────── */

const tip = h("div", { class: "tooltip" });
document.body.appendChild(tip);

function showTip(event, head, rows) {
  tip.textContent = "";
  add(tip, [
    h("div", { class: "tt-head" }, ...(Array.isArray(head) ? head.map((part) => h("span", { text: part })) : [h("span", { text: head })])),
    h("dl", {}, ...rows.flatMap(([key, value, klass]) => [h("dt", { text: key }), h("dd", { class: klass || "", text: value })])),
  ]);
  tip.classList.add("on");
  moveTip(event);
}
function moveTip(event) {
  const box = tip.getBoundingClientRect();
  let left = event.clientX + 14;
  let top = event.clientY + 14;
  if (left + box.width > window.innerWidth - 8) left = event.clientX - box.width - 14;
  if (top + box.height > window.innerHeight - 8) top = event.clientY - box.height - 14;
  tip.style.left = Math.max(8, left) + "px";
  tip.style.top = Math.max(8, top) + "px";
}
function hideTip() {
  tip.classList.remove("on");
}

/* ── Chart mounting ──────────────────────────────────────────────────── */

/**
 * Draw into `container`, and draw again whenever it changes width.
 *
 * Charts are sized from the DOM rather than given a fixed width so the page is
 * responsive without media queries per panel; the redraw is cheap because every
 * series is already computed.
 */
function mount(container, height, draw) {
  const render = () => {
    const width = Math.max(container.clientWidth, 220);
    container.textContent = "";
    const svg = svgEl("svg", {
      viewBox: `0 0 ${width} ${height}`,
      height,
      preserveAspectRatio: "none",
      role: "img",
    });
    container.appendChild(svg);
    draw(svg, width, height);
  };
  render();
  new ResizeObserver(debounce(render, 80)).observe(container);
  return container;
}
function debounce(fn, wait) {
  let timer = null;
  let lastWidth = -1;
  return (entries) => {
    const width = entries && entries[0] ? Math.round(entries[0].contentRect.width) : -1;
    if (width === lastWidth) return;
    lastWidth = width;
    clearTimeout(timer);
    timer = setTimeout(fn, wait);
  };
}

/** Axes and grid shared by every rectangular chart. */
function frame(svg, width, height, opts) {
  const pad = Object.assign({ left: 8, right: 54, top: 10, bottom: 22 }, opts.pad || {});
  const plot = {
    x0: pad.left,
    x1: width - pad.right,
    y0: pad.top,
    y1: height - pad.bottom,
  };
  plot.w = plot.x1 - plot.x0;
  plot.h = plot.y1 - plot.y0;

  const [lo, hi] = opts.domain;
  const y = scale(lo, hi, plot.y1, plot.y0);
  for (const value of niceTicks(lo, hi, opts.yTicks || 4)) {
    const at = y(value);
    svg.appendChild(svgEl("line", { class: "grid-line", x1: plot.x0, x2: plot.x1, y1: at, y2: at }));
    svg.appendChild(
      svgEl("text", {
        class: "axis-text",
        x: plot.x1 + 6,
        y: at + 3.5,
        text: (opts.format || compact)(value),
      })
    );
  }
  if (lo < 0 && hi > 0) {
    svg.appendChild(svgEl("line", { class: "zero-line", x1: plot.x0, x2: plot.x1, y1: y(0), y2: y(0) }));
  }
  return { plot, y };
}

function xLabels(svg, plot, labels) {
  for (const [at, text] of labels) {
    svg.appendChild(
      svgEl("text", { class: "axis-text mid", x: at, y: plot.y1 + 15, text })
    );
  }
}

/** A transparent rect that turns pointer position into the nearest datum. */
function hover(svg, plot, locate, onEnter) {
  const crossX = svgEl("line", { class: "crosshair", y1: plot.y0, y2: plot.y1, opacity: 0 });
  const crossY = svgEl("line", { class: "crosshair", x1: plot.x0, x2: plot.x1, opacity: 0 });
  svg.appendChild(crossX);
  svg.appendChild(crossY);
  const rect = svgEl("rect", {
    class: "hit",
    x: plot.x0,
    y: plot.y0,
    width: Math.max(plot.w, 1),
    height: Math.max(plot.h, 1),
  });
  rect.addEventListener("mousemove", (event) => {
    const box = svg.getBoundingClientRect();
    const px = ((event.clientX - box.left) / box.width) * svg.viewBox.baseVal.width;
    const found = locate(px);
    if (!found) return hideTip();
    crossX.setAttribute("x1", found.x);
    crossX.setAttribute("x2", found.x);
    crossX.setAttribute("opacity", 1);
    if (found.y !== undefined) {
      crossY.setAttribute("y1", found.y);
      crossY.setAttribute("y2", found.y);
      crossY.setAttribute("opacity", 1);
    }
    onEnter(event, found);
  });
  rect.addEventListener("mouseleave", () => {
    crossX.setAttribute("opacity", 0);
    crossY.setAttribute("opacity", 0);
    hideTip();
  });
  svg.appendChild(rect);
}

/* ── The performance curve ───────────────────────────────────────────
 *
 * TradingView's headline chart, with its four overlays. The one that earns its
 * place is "trades excursion": the equity line only moves when a trade closes,
 * so on its own it hides how far each position travelled before it got there. A
 * smooth curve made of trades that each spent a day 2R underwater is not a
 * smooth strategy.
 */

const PERF_SERIES = [
  { key: "cum", label: "Cumulative P&L", swatch: "swatch-up", on: true },
  { key: "hold", label: "Buy and hold", swatch: "swatch-muted", on: true },
  { key: "excursion", label: "Trades excursion", swatch: "swatch-blue", on: false },
  { key: "underwater", label: "Run-ups and drawdowns", swatch: "swatch-down", on: true },
];

function performanceChart(container, view, enabled) {
  const points = view.curve.points;
  const hold = view.benchmark.available ? view.benchmark.points : [];
  if (!points.length) return container.appendChild(h("p", { class: "empty", text: "No trades to plot." }));

  const series = points.map((point) => ({
    t: ms(point.t),
    cum: point.cum,
    base: point.cum - point.pnl,
    peak: point.peak - view.curve.start_equity,
    point,
  }));
  const holdSeries = hold.map((point) => ({ t: ms(point.t), pnl: point.pnl }));

  const from = Math.min(series[0].t, holdSeries.length ? holdSeries[0].t : Infinity);
  const to = Math.max(series[series.length - 1].t, holdSeries.length ? holdSeries[holdSeries.length - 1].t : -Infinity);

  const values = [0];
  if (enabled.cum || enabled.underwater) for (const row of series) values.push(row.cum, row.peak);
  if (enabled.hold) for (const row of holdSeries) values.push(row.pnl);
  if (enabled.excursion)
    for (const row of series) values.push(row.base + row.point.runup, row.base + row.point.adverse);
  const [lo, hi] = padDomain(Math.min(...values), Math.max(...values));

  mount(container, 300, (svg, width, height) => {
    const { plot, y } = frame(svg, width, height, { domain: [lo, hi], yTicks: 5, format: compact });
    const x = scale(from, to, plot.x0, plot.x1);

    const format = timeFormatter(to - from);
    xLabels(svg, plot, timeTicks(from, to, 6).map((value) => [x(value), format(value)]));

    if (enabled.excursion) {
      const step = Math.max(2, Math.min(9, plot.w / Math.max(series.length, 1) - 1));
      for (const row of series) {
        const top = y(row.base + row.point.runup);
        const bottom = y(row.base + row.point.adverse);
        svg.appendChild(
          svgEl("rect", {
            class: "fill-blue-soft",
            x: x(row.t) - step / 2,
            y: Math.min(top, bottom),
            width: step,
            height: Math.max(Math.abs(bottom - top), 1),
            rx: 1,
          })
        );
      }
    }

    if (enabled.underwater) {
      const forward = series.map((row) => [x(row.t), y(row.peak)]);
      const back = series.map((row) => [x(row.t), y(row.cum)]).reverse();
      svg.appendChild(svgEl("path", { class: "fill-down-soft", d: linePath(forward.concat(back)) + " Z" }));
    }

    if (enabled.hold && holdSeries.length) {
      svg.appendChild(
        svgEl("path", {
          class: "stroke-muted",
          "stroke-width": 1.25,
          "stroke-dasharray": "4 3",
          d: linePath(holdSeries.map((row) => [x(row.t), y(row.pnl)])),
        })
      );
    }

    if (enabled.cum) {
      const path = series.map((row) => [x(row.t), y(row.cum)]);
      path.unshift([x(from), y(0)]);
      const positive = series[series.length - 1].cum >= 0;
      const paint = signGradient(svg, plot, y, lo, hi);
      svg.appendChild(
        svgEl("path", {
          class: paint ? null : positive ? "fill-up-soft" : "fill-down-soft",
          fill: paint,
          "fill-opacity": paint ? 0.19 : null,
          d: linePath(path) + ` L ${x(to).toFixed(2)} ${y(0).toFixed(2)} Z`,
        })
      );
      svg.appendChild(
        svgEl("path", {
          class: paint ? null : positive ? "stroke-up" : "stroke-down",
          stroke: paint,
          fill: paint ? "none" : null,
          "stroke-width": 1.6,
          d: linePath(path),
        })
      );
      for (const row of series) {
        svg.appendChild(
          svgEl("circle", {
            class: row.point.pnl >= 0 ? "series-up" : "series-down",
            cx: x(row.t),
            cy: y(row.cum),
            r: series.length > 160 ? 1.3 : 2.1,
          })
        );
      }
    }

    hover(
      svg,
      plot,
      (px) => {
        let best = null;
        for (const row of series) {
          const distance = Math.abs(x(row.t) - px);
          if (!best || distance < best.distance) best = { distance, row, x: x(row.t), y: y(row.cum) };
        }
        return best;
      },
      (event, found) => {
        const point = found.row.point;
        showTip(
          event,
          [`#${point.i} ${point.dir}`, dateLabel(point.t, true)],
          [
            ["Trade P&L", money(point.pnl), tone(point.pnl)],
            ["Cumulative", money(point.cum), tone(point.cum)],
            ["R", rMultiple(point.r), tone(point.r)],
            ["Run-up / MAE", `${compact(point.runup)} / ${compact(point.adverse)}`],
            ["Drawdown", money(point.drawdown), point.drawdown < 0 ? "down" : ""],
            ["Exit", point.reason || NA],
            ["Bars held", num(point.bars, 0)],
          ]
        );
      }
    );
  });
}

/* ── Price and trades ────────────────────────────────────────────────
 *
 * Bars are spaced by index, not by timestamp: a weekend is not four days of
 * flat price, it is an absence of bars, and spacing by time draws it as a
 * plateau that never traded. Trade markers are placed at the fractional index
 * their timestamp falls between, which is why they still line up with the
 * candles either side of a gap.
 */

function priceChart(container, view) {
  const rows = view.market.rows;
  if (!rows.length) return container.appendChild(h("p", { class: "empty", text: "No price window in this report." }));
  const times = rows.map((row) => ms(row[0]));
  const highs = rows.map((row) => row[2]);
  const lows = rows.map((row) => row[3]);
  const [lo, hi] = padDomain(Math.min(...lows), Math.max(...highs), 0.05);

  const at = (moment) => {
    const value = ms(moment);
    if (!Number.isFinite(value)) return null;
    if (value <= times[0]) return 0;
    if (value >= times[times.length - 1]) return times.length - 1;
    let low = 0;
    let high = times.length - 1;
    while (high - low > 1) {
      const mid = (low + high) >> 1;
      if (times[mid] <= value) low = mid;
      else high = mid;
    }
    const span = times[high] - times[low] || 1;
    return low + (value - times[low]) / span;
  };

  mount(container, 320, (svg, width, height) => {
    const { plot, y } = frame(svg, width, height, {
      domain: [lo, hi],
      yTicks: 5,
      format: price,
    });
    const x = scale(0, rows.length - 1, plot.x0 + 3, plot.x1 - 3);
    const bar = Math.max(1, Math.min(8, (plot.w / rows.length) * 0.68));

    const format = timeFormatter(times[times.length - 1] - times[0]);
    xLabels(
      svg,
      plot,
      timeTicks(0, rows.length - 1, 6).map((index) => [x(index), format(times[Math.round(index)])])
    );

    rows.forEach((row, index) => {
      const rising = row[4] >= row[1];
      const cx = x(index);
      svg.appendChild(
        svgEl("line", {
          class: rising ? "candle-up" : "candle-down",
          "stroke-width": Math.max(0.7, bar * 0.16),
          x1: cx,
          x2: cx,
          y1: y(row[2]),
          y2: y(row[3]),
        })
      );
      const top = y(Math.max(row[1], row[4]));
      const bottom = y(Math.min(row[1], row[4]));
      svg.appendChild(
        svgEl("rect", {
          class: rising ? "candle-up" : "candle-down",
          x: cx - bar / 2,
          y: top,
          width: bar,
          height: Math.max(bottom - top, 0.8),
        })
      );
    });

    for (const trade of view.market.trades) {
      const entryIndex = at(trade.entry_t);
      const exitIndex = at(trade.exit_t);
      if (entryIndex === null) continue;
      const won = trade.pnl > 0;
      if (exitIndex !== null && trade.exit !== null) {
        svg.appendChild(
          svgEl("line", {
            class: won ? "stroke-up" : "stroke-down",
            "stroke-width": 1,
            "stroke-dasharray": "3 2",
            opacity: 0.75,
            x1: x(entryIndex),
            y1: y(trade.entry),
            x2: x(exitIndex),
            y2: y(trade.exit),
          })
        );
      }
      const cx = x(entryIndex);
      const cy = y(trade.entry);
      const size = 4.2;
      const up = trade.dir === "LONG";
      const marker = svgEl("path", {
        class: (up ? "series-up" : "series-down") + " marker",
        d: up
          ? `M ${cx} ${cy + size * 1.6} l ${size} ${size * 1.4} l ${-size * 2} 0 Z`
          : `M ${cx} ${cy - size * 1.6} l ${size} ${-size * 1.4} l ${-size * 2} 0 Z`,
      });
      marker.addEventListener("mousemove", (event) =>
        showTip(
          event,
          [`#${trade.i} ${trade.dir}`, dateLabel(trade.entry_t, true)],
          [
            ["Entry", price(trade.entry)],
            ["Exit", price(trade.exit)],
            ["Stop", price(trade.sl)],
            ["P&L", money(trade.pnl), tone(trade.pnl)],
            ["R", rMultiple(trade.r), tone(trade.r)],
            ["Exit reason", trade.reason || NA],
          ]
        )
      );
      marker.addEventListener("mouseleave", hideTip);
      svg.appendChild(marker);
    }

    hover(
      svg,
      plot,
      (px) => {
        const index = Math.max(0, Math.min(rows.length - 1, Math.round((px - plot.x0 - 3) / ((plot.w - 6) / (rows.length - 1 || 1)))));
        return { index, x: x(index), y: y(rows[index][4]) };
      },
      (event, found) => {
        const row = rows[found.index];
        showTip(event, [view.meta.symbol, dateLabel(row[0], true)], [
          ["Open", price(row[1])],
          ["High", price(row[2])],
          ["Low", price(row[3])],
          ["Close", price(row[4])],
        ]);
      }
    );
  });
}

/* ── Categorical bars ────────────────────────────────────────────────── */

function barSeries(container, rows, opts) {
  if (!rows.length) return container.appendChild(h("p", { class: "empty", text: "Nothing to show." }));
  const values = rows.flatMap((row) => opts.values(row));
  const [lo, hi] = padDomain(Math.min(0, ...values), Math.max(0, ...values));

  mount(container, opts.height || 220, (svg, width, height) => {
    const { plot, y } = frame(svg, width, height, {
      domain: [lo, hi],
      yTicks: 4,
      format: opts.format || compact,
    });
    const step = plot.w / rows.length;
    const bar = Math.max(1.5, Math.min(opts.maxBar || 22, step * 0.66));

    rows.forEach((row, index) => {
      const cx = plot.x0 + step * (index + 0.5);
      for (const part of opts.parts(row)) {
        if (!part.value) continue;
        const top = y(Math.max(part.value, 0));
        const bottom = y(Math.min(part.value, 0));
        const offset = part.offset || 0;
        svg.appendChild(
          svgEl("rect", {
            class: part.class,
            x: cx - bar / 2 + offset * bar,
            y: top,
            width: part.width ? bar * part.width : bar,
            height: Math.max(bottom - top, 1),
            rx: 1.5,
            opacity: part.opacity,
          })
        );
      }
    });

    const every = Math.max(1, Math.ceil(rows.length / (opts.labelCount || 8)));
    xLabels(
      svg,
      plot,
      rows.flatMap((row, index) =>
        index % every === 0 ? [[plot.x0 + step * (index + 0.5), opts.label(row, index)]] : []
      )
    );

    hover(
      svg,
      plot,
      (px) => {
        const index = Math.max(0, Math.min(rows.length - 1, Math.floor((px - plot.x0) / step)));
        return { index, x: plot.x0 + step * (index + 0.5) };
      },
      (event, found) => opts.tip(event, rows[found.index])
    );
  });
}

/* ── Histogram, donut, diverging and comparison bars ─────────────────── */

function histogramChart(container, distribution) {
  const bins = distribution.bins;
  if (!bins.length) return container.appendChild(h("p", { class: "empty", text: "No trades to bin." }));
  const unit = distribution.unit === "percent" ? "%" : "";
  const label = (value) => num(value, Math.abs(value) < 1 ? 2 : 1) + unit;
  const top = Math.max(...bins.map((bin) => bin.winners + bin.losers));

  mount(container, 220, (svg, width, height) => {
    const { plot, y } = frame(svg, width, height, {
      domain: [0, top * 1.12 || 1],
      yTicks: 4,
      format: (value) => num(value, 0),
      pad: { left: 8, right: 54, top: 20, bottom: 22 },
    });
    const step = plot.w / bins.length;
    bins.forEach((bin, index) => {
      const x0 = plot.x0 + step * index + 1;
      const w = Math.max(step - 2, 1);
      let base = plot.y1;
      for (const [count, klass] of [[bin.losers, "series-down"], [bin.winners, "series-up"]]) {
        if (!count) continue;
        const barHeight = plot.y1 - y(count);
        base -= barHeight;
        svg.appendChild(svgEl("rect", { class: klass, x: x0, y: base, width: w, height: barHeight, rx: 1.5 }));
      }
    });

    for (const [value, klass, text] of [
      [distribution.average_loss, "stroke-down", "avg loss"],
      [distribution.average_win, "stroke-up", "avg profit"],
    ]) {
      if (value === null || value === undefined) continue;
      const lo = bins[0].from;
      const hi = bins[bins.length - 1].to;
      const at = plot.x0 + ((value - lo) / (hi - lo || 1)) * plot.w;
      if (at < plot.x0 || at > plot.x1) continue;
      svg.appendChild(
        svgEl("line", { class: klass, "stroke-width": 1, "stroke-dasharray": "3 3", x1: at, x2: at, y1: plot.y0, y2: plot.y1 })
      );
      svg.appendChild(svgEl("text", { class: "axis-text mid", x: at, y: plot.y0 - 6, text: `${text} ${label(value)}` }));
    }

    const every = Math.max(1, Math.ceil(bins.length / 7));
    xLabels(
      svg,
      plot,
      bins.flatMap((bin, index) => (index % every === 0 ? [[plot.x0 + step * (index + 0.5), label(bin.from)]] : []))
    );

    hover(
      svg,
      plot,
      (px) => {
        const index = Math.max(0, Math.min(bins.length - 1, Math.floor((px - plot.x0) / step)));
        return { index, x: plot.x0 + step * (index + 0.5) };
      },
      (event, found) => {
        const bin = bins[found.index];
        showTip(event, `${label(bin.from)} → ${label(bin.to)}`, [
          ["Winners", num(bin.winners, 0), "up"],
          ["Losers", num(bin.losers, 0), "down"],
        ]);
      }
    );
  });
}

function donutChart(container, split) {
  const slices = [
    { label: "Winners", value: split.winners, klass: "series-up" },
    { label: "Losers", value: split.losers, klass: "series-down" },
    { label: "Breakevens", value: split.breakeven, klass: "series-muted" },
  ];
  const total = split.total || 1;

  mount(container, 210, (svg, width, height) => {
    const radius = Math.min(width, height) / 2 - 12;
    const cx = Math.min(width / 2, 110);
    const cy = height / 2;
    let angle = -Math.PI / 2;
    for (const slice of slices) {
      if (!slice.value) continue;
      const sweep = (slice.value / total) * Math.PI * 2;
      const end = angle + sweep;
      const large = sweep > Math.PI ? 1 : 0;
      const inner = radius * 0.62;
      const path = [
        `M ${cx + radius * Math.cos(angle)} ${cy + radius * Math.sin(angle)}`,
        `A ${radius} ${radius} 0 ${large} 1 ${cx + radius * Math.cos(end)} ${cy + radius * Math.sin(end)}`,
        `L ${cx + inner * Math.cos(end)} ${cy + inner * Math.sin(end)}`,
        `A ${inner} ${inner} 0 ${large} 0 ${cx + inner * Math.cos(angle)} ${cy + inner * Math.sin(angle)}`,
        "Z",
      ].join(" ");
      const arc = svgEl("path", { class: slice.klass, d: path });
      arc.addEventListener("mousemove", (event) =>
        showTip(event, slice.label, [
          ["Trades", num(slice.value, 0)],
          ["Share", pct((100 * slice.value) / total)],
        ])
      );
      arc.addEventListener("mouseleave", hideTip);
      svg.appendChild(arc);
      angle = end;
    }
    svg.appendChild(svgEl("text", { class: "axis-text mid", x: cx, y: cy - 2, style: "font-size:19px;font-weight:600", fill: "var(--text-strong)", text: String(split.total) }));
    svg.appendChild(svgEl("text", { class: "axis-text mid", x: cx, y: cy + 14, text: "Total trades" }));

    const legendX = cx + radius + 24;
    slices.forEach((slice, index) => {
      const y = cy - 26 + index * 22;
      svg.appendChild(svgEl("circle", { class: slice.klass, cx: legendX, cy: y - 4, r: 4 }));
      svg.appendChild(svgEl("text", { class: "axis-text", x: legendX + 12, y: y, text: slice.label }));
      svg.appendChild(
        svgEl("text", { class: "axis-text", x: legendX + 100, y: y, text: `${slice.value}  ${pct((100 * slice.value) / total)}` })
      );
    });
  });
}

/** Loss to the left, profit to the right, net at the end — TradingView's shape. */
function divergingBars(container, rows) {
  if (!rows.length) return container.appendChild(h("p", { class: "empty", text: "Nothing to group." }));
  const reach = Math.max(...rows.map((row) => Math.max(row.profit, row.loss)), 1);

  mount(container, Math.max(70, rows.length * 27 + 16), (svg, width) => {
    const labelWidth = Math.min(190, Math.max(90, width * 0.22));
    const valueWidth = 84;
    const mid = labelWidth + (width - labelWidth - valueWidth) / 2;
    const half = (width - labelWidth - valueWidth) / 2 - 6;

    rows.forEach((row, index) => {
      const y = 14 + index * 27;
      svg.appendChild(svgEl("text", { class: "axis-text", x: 0, y: y + 4, text: row.name }));
      const lossWidth = (row.loss / reach) * half;
      const profitWidth = (row.profit / reach) * half;
      if (lossWidth > 0)
        svg.appendChild(svgEl("rect", { class: "series-down", x: mid - lossWidth, y: y - 5, width: lossWidth, height: 11, rx: 2 }));
      if (profitWidth > 0)
        svg.appendChild(svgEl("rect", { class: "series-up", x: mid, y: y - 5, width: profitWidth, height: 11, rx: 2 }));
      svg.appendChild(
        svgEl("text", {
          class: "axis-text end",
          x: width,
          y: y + 4,
          fill: row.net >= 0 ? "var(--up)" : "var(--down)",
          text: money(row.net),
        })
      );
      const hit = svgEl("rect", { class: "hit", x: 0, y: y - 12, width: width, height: 25 });
      hit.addEventListener("mousemove", (event) =>
        showTip(event, row.name, [
          ["Trades", num(row.trades, 0)],
          ["Win rate", pct(row.win_rate)],
          ["Gross profit", money(row.profit), "up"],
          ["Gross loss", money(-row.loss), "down"],
          ["Net", money(row.net), tone(row.net)],
        ])
      );
      hit.addEventListener("mouseleave", hideTip);
      svg.appendChild(hit);
    });
    svg.appendChild(svgEl("line", { class: "zero-line", x1: mid, x2: mid, y1: 2, y2: rows.length * 27 + 8 }));
  });
}

/** Maximum / average / current, as a labelled horizontal bar each. */
function compareBars(container, groups) {
  const reach = Math.max(...groups.flatMap((group) => group.rows.map((row) => Math.abs(row.value || 0))), 1);
  const total = groups.reduce((sum, group) => sum + group.rows.length, 0);

  mount(container, groups.length * 26 + total * 22 + 8, (svg, width) => {
    let y = 12;
    for (const group of groups) {
      svg.appendChild(svgEl("text", { class: "axis-text", x: 0, y, style: "font-weight:600", fill: "var(--text-strong)", text: group.title }));
      y += 16;
      for (const row of group.rows) {
        const barWidth = ((Math.abs(row.value) || 0) / reach) * (width - 160);
        svg.appendChild(svgEl("text", { class: "axis-text", x: 0, y: y + 4, text: row.label }));
        svg.appendChild(svgEl("rect", { class: group.klass, x: 72, y: y - 5, width: Math.max(barWidth, row.value ? 2 : 0), height: 10, rx: 2, opacity: row.faded ? 0.55 : 1 }));
        svg.appendChild(svgEl("text", { class: "axis-text end", x: width, y: y + 4, text: pct(row.value) }));
        y += 22;
      }
      y += 10;
    }
  });
}

function scatterChart(container, points, opts) {
  if (!points.length) return container.appendChild(h("p", { class: "empty", text: "No trade carries an R-multiple." }));
  const xs = points.map(opts.x).filter(Number.isFinite);
  const ys = points.map(opts.y).filter(Number.isFinite);
  const [xLo, xHi] = padDomain(Math.min(0, ...xs), Math.max(...xs));
  const [yLo, yHi] = padDomain(Math.min(...ys), Math.max(...ys));

  mount(container, 280, (svg, width, height) => {
    const { plot, y } = frame(svg, width, height, {
      domain: [yLo, yHi],
      yTicks: 4,
      format: (value) => num(value, 1) + "R",
      pad: { left: 8, right: 46, top: 12, bottom: 42 },
    });
    const x = scale(xLo, xHi, plot.x0, plot.x1);
    xLabels(svg, plot, niceTicks(xLo, xHi, 5).map((value) => [x(value), num(value, 1) + "R"]));
    if (xLo < 1 && xHi > 1) {
      svg.appendChild(svgEl("line", { class: "grid-line", x1: x(1), x2: x(1), y1: plot.y0, y2: plot.y1 }));
    }
    for (const point of points) {
      const px = opts.x(point);
      const py = opts.y(point);
      if (!Number.isFinite(px) || !Number.isFinite(py)) continue;
      const dot = svgEl("circle", {
        class: py > 0 ? "series-up" : "series-down",
        cx: x(px),
        cy: y(py),
        r: 3.2,
        opacity: 0.78,
      });
      dot.addEventListener("mousemove", (event) =>
        showTip(event, [`#${point.i} ${point.dir}`, dateLabel(point.t)], [
          ["R", rMultiple(point.r), tone(point.r)],
          ["MAE", rMultiple(point.mae_r)],
          ["MFE", rMultiple(point.mfe_r)],
          ["P&L", money(point.pnl), tone(point.pnl)],
          ["Exit", point.reason || NA],
        ])
      );
      dot.addEventListener("mouseleave", hideTip);
      svg.appendChild(dot);
    }
    svg.appendChild(svgEl("text", { class: "axis-text mid", x: (plot.x0 + plot.x1) / 2, y: height - 2, text: opts.xLabel }));
  });
}

/* ── Page furniture ──────────────────────────────────────────────────── */

function panel(title, hint, extra) {
  const body = h("div", { class: "body" });
  const header = h("header", {}, h("h2", { text: title }), hint ? h("span", { class: "hint", text: hint }) : null);
  if (extra) header.appendChild(extra);
  return { node: h("section", { class: "panel" }, header, body), body, header };
}

/** Pill tabs that swap one body for another, TradingView's own navigation. */
function tabbed(section, tabs, hint) {
  const bar = h("div", { class: "tabs" });
  const body = h("div", { class: "body" });
  const buttons = tabs.map((tab, index) =>
    h("button", {
      class: "tab",
      type: "button",
      role: "tab",
      "aria-selected": String(index === 0),
      text: tab.label,
      onclick: () => select(index),
    })
  );
  function select(index) {
    buttons.forEach((button, position) => button.setAttribute("aria-selected", String(position === index)));
    body.textContent = "";
    tabs[index].render(body);
  }
  add(bar, buttons);
  section.node.insertBefore(bar, section.body);
  section.node.replaceChild(body, section.body);
  section.body = body;
  select(0);
  return section;
}

function kpis(items) {
  return h(
    "div",
    { class: "kpis" },
    ...items.map(([label, value, klass]) =>
      h("div", { class: "kpi" }, h("div", { class: "k", text: label }), h("div", { class: "v " + (klass || ""), text: value }))
    )
  );
}

function chartBox(title, height) {
  const box = h("div", { class: "chart" });
  return { node: h("div", {}, title ? h("div", { class: "chart-title", text: title }) : null, box), box, height };
}

function legendFor(series, onChange) {
  const state = Object.fromEntries(series.map((entry) => [entry.key, entry.on]));
  const bar = h("div", { class: "legend" });
  for (const entry of series) {
    const button = h(
      "button",
      {
        type: "button",
        class: entry.swatch,
        "aria-pressed": String(state[entry.key]),
        onclick: () => {
          state[entry.key] = !state[entry.key];
          button.setAttribute("aria-pressed", String(state[entry.key]));
          onChange(state);
        },
      },
      h("span", { class: "swatch" }),
      h("span", { text: entry.label })
    );
    bar.appendChild(button);
  }
  return { bar, state };
}

function segmented(options, onChange, active) {
  const bar = h("div", { class: "seg" });
  const selected = active === undefined ? 0 : active;
  const buttons = options.map((option, index) =>
    h("button", {
      type: "button",
      text: option.label,
      "aria-pressed": String(index === selected),
      onclick: () => {
        buttons.forEach((other, position) => other.setAttribute("aria-pressed", String(position === index)));
        onChange(option.value);
      },
    })
  );
  add(bar, buttons);
  return bar;
}

function table(columns, rows, opts) {
  const options = opts || {};
  let sort = options.sort || null;
  let ascending = options.ascending !== false;

  const head = h(
    "tr",
    {},
    ...columns.map((column) =>
      h("th", {
        text: column.label,
        class: column.sortable === false ? "" : "sortable",
        onclick: column.sortable === false ? null : () => {
          ascending = sort === column.key ? !ascending : false;
          sort = column.key;
          draw();
        },
      })
    )
  );
  const body = h("tbody");
  const node = h("table", { class: "data" }, h("thead", {}, head), body);

  function draw() {
    const ordered = rows.slice();
    if (sort) {
      const column = columns.find((entry) => entry.key === sort);
      ordered.sort((left, right) => {
        const a = column.value(left);
        const b = column.value(right);
        if (a === b) return 0;
        if (a === null || a === undefined) return 1;
        if (b === null || b === undefined) return -1;
        const order = a > b ? 1 : -1;
        return ascending ? order : -order;
      });
    }
    head.querySelectorAll("th").forEach((cell, index) => {
      cell.classList.toggle("sorted", columns[index].key === sort);
      cell.classList.toggle("asc", columns[index].key === sort && ascending);
    });
    body.textContent = "";
    for (const row of ordered) {
      const tr = h("tr", {});
      for (const column of columns) {
        const cell = column.cell(row);
        tr.appendChild(typeof cell === "string" ? h("td", { class: column.numeric === false ? "" : "num", text: cell }) : cell);
      }
      body.appendChild(tr);
    }
  }
  draw();
  return { node, redraw: (next) => { rows = next; draw(); } };
}

/* ── Panels ──────────────────────────────────────────────────────────── */

function masthead(view) {
  const meta = view.meta;
  const counts = view.diagnostics.counts || {};
  const critical = counts.critical || 0;
  const bar = h(
    "div",
    { class: "masthead" },
    h(
      "div",
      {},
      h("h1", { text: `${meta.strategy} — ${meta.symbol} ${meta.timeframe}` }),
      h("div", {
        class: "sub",
        text: `${dateLabel(meta.first_bar)} → ${dateLabel(meta.last_bar)} · ${num(meta.bars, 0)} bars · ${num(
          meta.warmup_bars,
          0
        )} of warm-up · generated ${dateLabel(meta.generated_at, true)} UTC`,
      })
    ),
    h("span", { class: "spacer" }),
    critical
      ? h("span", { class: "chip warn", text: `${critical} critical finding${critical > 1 ? "s" : ""}` })
      : h("span", { class: "chip ok", text: "no critical findings" }),
    h("span", { class: "chip mono", text: `schema ${meta.schema_version}` }),
    h("button", { class: "icon-btn", type: "button", text: "Theme", onclick: toggleTheme }),
    h("button", { class: "icon-btn", type: "button", text: "Print", onclick: () => window.print() })
  );
  return bar;
}

function statTiles(view) {
  return h(
    "div",
    { class: "stats" },
    ...view.headline.map((stat) =>
      h(
        "div",
        { class: "stat" },
        h("div", { class: "k", text: stat.label }),
        h("div", {
          class: "v " + stat.tone,
          text: stat.kind === "percent" ? pct(stat.value) : stat.kind === "ratio" ? num(stat.value, 3) : money(stat.value),
        }),
        h("div", { class: "s", text: stat.sub || "" })
      )
    )
  );
}

function performancePanel(view) {
  const section = panel("Performance", "Equity moves only when a trade closes — the excursion overlay is what happened in between");
  const box = h("div", { class: "chart" });
  const { bar, state } = legendFor(PERF_SERIES, (next) => {
    box.textContent = "";
    performanceChart(box, view, next);
  });
  section.body.appendChild(bar);
  section.body.appendChild(box);
  performanceChart(box, view, state);
  return section.node;
}

function pricePanel(view) {
  if (!view.market.available) return null;
  const section = panel(
    "Price and trades",
    `${view.market.rows.length} candles, each aggregating ${view.market.bucket_bars} bar${
      view.market.bucket_bars > 1 ? "s" : ""
    } — a drawing of the window, not the data the metrics came from`
  );
  const box = h("div", { class: "chart" });
  section.body.appendChild(box);
  priceChart(box, view);
  return section.node;
}

function performanceAnalysis(view) {
  const section = panel("Performance analysis");
  const tabs = [
    { label: "Breakdown", render: (body) => breakdownTab(body, view) },
    { label: "Periodical", render: (body) => periodicalTab(body, view) },
  ];
  if (view.benchmarking.available) tabs.push({ label: "Benchmarking", render: (body) => benchmarkTab(body, view) });
  if (view.growth.available) tabs.push({ label: "Growth and decline", render: (body) => growthTab(body, view) });
  return tabbed(section, tabs).node;
}

function breakdownTab(body, view) {
  const data = view.breakdown;
  body.appendChild(
    kpis([
      ["Gross profit", money(data.gross_profit), "up"],
      ["Gross loss", money(-data.gross_loss), "down"],
      ["Profit factor", num(data.profit_factor, 3)],
      ["Commission load", pct(data.commission_load)],
      ["Net P&L", money(data.net_pnl), tone(data.net_pnl)],
    ])
  );
  const names = Object.keys(data.groups);
  const box = h("div", { class: "chart" });
  const title = h("div", { class: "chart-title", text: "Profits and losses" });
  const controls = segmented(
    names.map((name) => ({ label: name, value: name })),
    (name) => {
      box.textContent = "";
      divergingBars(box, data.groups[name]);
    }
  );
  body.appendChild(h("div", { style: "display:flex;align-items:center;gap:10px;justify-content:space-between;margin-top:10px" }, title, controls));
  body.appendChild(box);
  divergingBars(box, data.groups[names[0]]);
}

function periodicalTab(body, view) {
  const data = view.periodical;
  body.appendChild(
    kpis([
      ["Annualised return (CAGR)", pct(data.cagr), tone(data.cagr)],
      ["Total return", pct(data.total_return_pct), tone(data.total_return_pct)],
      ["Sharpe (per trade)", num(data.sharpe, 3)],
      ["Sortino (per trade)", num(data.sortino, 3)],
    ])
  );
  const box = h("div", { class: "chart" });
  const controls = segmented(
    [
      { label: "Daily", value: "day" },
      { label: "Weekly", value: "week" },
      { label: "Monthly", value: "month" },
      { label: "Yearly", value: "year" },
    ],
    (period) => drawPeriod(period),
    2
  );
  body.appendChild(
    h(
      "div",
      { style: "display:flex;align-items:center;gap:10px;justify-content:space-between;margin-top:10px" },
      h("div", { class: "chart-title", text: "P&L by period" }),
      controls
    )
  );
  body.appendChild(box);

  function drawPeriod(period) {
    box.textContent = "";
    const rows = data.buckets[period];
    barSeries(box, rows, {
      height: 240,
      values: (row) => [row.profit, row.loss, row.mfe, row.mae],
      parts: (row) => [
        { value: row.mfe, class: "series-up", opacity: 0.25, width: 0.42, offset: 0.29 },
        { value: row.mae, class: "series-down", opacity: 0.25, width: 0.42, offset: 0.29 },
        { value: row.profit, class: "series-up" },
        { value: row.loss, class: "series-down" },
      ],
      label: (row) => row.t,
      tip: (event, row) =>
        showTip(event, row.t, [
          ["Trades", num(row.trades, 0)],
          ["Realised profit", money(row.profit), "up"],
          ["Realised loss", money(row.loss), "down"],
          ["Net", money(row.net), tone(row.net)],
          ["Favourable excursion", money(row.mfe)],
          ["Adverse excursion", money(row.mae)],
        ]),
    });
  }
  drawPeriod("month");
  body.appendChild(
    h("div", { class: "legend" },
      h("button", { class: "swatch-up", "aria-pressed": "true", type: "button", disabled: "" }, h("span", { class: "swatch" }), h("span", { text: "Realised profit" })),
      h("button", { class: "swatch-down", "aria-pressed": "true", type: "button", disabled: "" }, h("span", { class: "swatch" }), h("span", { text: "Realised loss" })),
      h("button", { class: "swatch-muted", "aria-pressed": "true", type: "button", disabled: "" }, h("span", { class: "swatch" }), h("span", { text: "Pale bars: excursion while open" }))
    )
  );
}

function benchmarkTab(body, view) {
  const data = view.benchmarking;
  body.appendChild(
    kpis([
      ["Strategy return", signedPct(data.strategy_return_pct), tone(data.strategy_return_pct)],
      ["Buy and hold return", signedPct(data.hold_return_pct), tone(data.hold_return_pct)],
      ["Outperformance", signedPct(data.outperformance_pct), tone(data.outperformance_pct)],
      ["Correlation (weekly P&L)", num(data.correlation, 3)],
    ])
  );
  const box = h("div", { class: "chart" });
  body.appendChild(h("div", { class: "chart-title", text: "Strategy vs buy and hold, week by week" }));
  body.appendChild(box);
  barSeries(box, data.series, {
    height: 230,
    values: (row) => [row.strategy, row.hold],
    parts: (row) => [
      { value: row.strategy, class: "series-blue", width: 0.5, offset: 0 },
      { value: row.hold, class: "series-muted", width: 0.5, offset: 0.5, opacity: 0.65 },
    ],
    label: (row) => row.t.slice(5),
    tip: (event, row) =>
      showTip(event, "Week of " + row.t, [
        ["Strategy", money(row.strategy), tone(row.strategy)],
        ["Buy and hold", money(row.hold), tone(row.hold)],
        ["Difference", money(row.strategy - row.hold), tone(row.strategy - row.hold)],
      ]),
  });
  body.appendChild(
    h("p", { class: "hint muted", text: "Buy and hold pays no spread, no slippage and no commission: it is the floor a strategy has to clear, not a like-for-like trade." })
  );
}

function growthTab(body, view) {
  const data = view.growth;
  body.appendChild(
    kpis([
      ["Average run-up duration", data.runup.avg_days === null ? NA : `${num(data.runup.avg_days, 1)} days`],
      ["Average drawdown duration", data.drawdown.avg_days === null ? NA : `${num(data.drawdown.avg_days, 1)} days`],
      ["Max drawdown", `${money(-(data.drawdown.max_amount || 0))} · ${pct(data.drawdown.max_pct)}`, "down"],
      ["Growth and decline periods", `${data.runup.count} up / ${data.drawdown.count} down`],
    ])
  );
  const left = h("div", { class: "chart" });
  const right = h("div", {});
  body.appendChild(
    h(
      "div",
      { class: "split even", style: "margin-top:10px" },
      h("div", {}, h("div", { class: "chart-title", text: "Alternating growth and decline" }), left),
      h("div", {}, h("div", { class: "chart-title", text: "Comparison of growth and decline periods" }), right)
    )
  );
  barSeries(left, data.segments, {
    height: 230,
    maxBar: 34,
    values: (row) => [row.kind === "runup" ? row.pct : -row.pct],
    parts: (row) => [
      {
        value: row.kind === "runup" ? row.pct : -row.pct,
        class: row.kind === "runup" ? "series-up" : "series-down",
        opacity: row.ongoing ? 0.6 : 1,
      },
    ],
    label: (row) => dateLabel(row.from).slice(5),
    labelCount: 7,
    tip: (event, row) =>
      showTip(event, row.kind === "runup" ? "Run-up" : "Drawdown", [
        ["From", dateLabel(row.from)],
        ["To", dateLabel(row.to)],
        ["Duration", row.days === null ? NA : `${num(row.days, 1)} days`],
        ["Change", signedPct(row.kind === "runup" ? row.pct : -row.pct), row.kind === "runup" ? "up" : "down"],
        ["Amount", money(row.kind === "runup" ? row.amount : -row.amount)],
        ["Status", row.ongoing ? "in progress at the last trade" : "closed"],
      ]),
  });
  compareBars(right, [
    {
      title: "Run-up",
      klass: "series-up",
      rows: [
        { label: "Maximum", value: data.runup.max_pct },
        { label: "Average", value: data.runup.avg_pct },
        { label: "Current", value: data.runup.current_pct, faded: true },
      ],
    },
    {
      title: "Drawdown",
      klass: "series-down",
      rows: [
        { label: "Maximum", value: data.drawdown.max_pct },
        { label: "Average", value: data.drawdown.avg_pct },
        { label: "Current", value: data.drawdown.current_pct, faded: true },
      ],
    },
  ]);
}

function tradesAnalysis(view) {
  const section = panel("Trades analysis");
  return tabbed(section, [
    { label: "Distribution", render: (body) => distributionTab(body, view) },
    { label: "Streaks", render: (body) => streaksTab(body, view) },
    { label: "Trades analysis details", render: (body) => detailsTab(body, view) },
  ]).node;
}

function distributionTab(body, view) {
  const data = view.distribution;
  body.appendChild(
    kpis([
      ["Expected payoff", money(data.expected_payoff), tone(data.expected_payoff)],
      ["Outliers P&L", `${money(data.outlier_pnl)} · ${data.outlier_count} trade${data.outlier_count === 1 ? "" : "s"}`],
      ["Largest profit", money(data.largest_profit), "up"],
      ["Largest loss", money(data.largest_loss), "down"],
    ])
  );
  const left = h("div", { class: "chart" });
  const right = h("div", { class: "chart" });
  body.appendChild(
    h(
      "div",
      { class: "split", style: "margin-top:10px" },
      h(
        "div",
        {},
        h("div", { class: "chart-title", text: data.unit === "percent" ? "Returns distribution (% of starting equity)" : "Returns distribution" }),
        left
      ),
      h("div", {}, h("div", { class: "chart-title", text: "Trades distribution" }), right)
    )
  );
  histogramChart(left, data);
  donutChart(right, data.split);
}

function streaksTab(body, view) {
  const data = view.streaks;
  body.appendChild(
    kpis([
      ["Longest winning streak", `${data.longest_win} trades`, "up"],
      ["Longest losing streak", `${data.longest_loss} trades`, "down"],
      ["Average winning streak", data.average_win === null ? NA : `${num(data.average_win, 1)} trades`],
      ["Average losing streak", data.average_loss === null ? NA : `${num(data.average_loss, 1)} trades`],
    ])
  );
  const box = h("div", { class: "chart" });
  let mode = "count";
  const controls = segmented(
    [
      { label: "Count", value: "count" },
      { label: "Amount", value: "amount" },
    ],
    (value) => {
      mode = value;
      redraw();
    }
  );
  body.appendChild(
    h(
      "div",
      { style: "display:flex;align-items:center;gap:10px;justify-content:space-between;margin-top:10px" },
      h("div", { class: "chart-title", text: "Winning and losing streaks" }),
      controls
    )
  );
  body.appendChild(box);

  function redraw() {
    box.textContent = "";
    barSeries(box, data.runs, {
      height: 230,
      values: (row) => [signedRun(row)],
      parts: (row) => [{ value: signedRun(row), class: row.kind === "win" ? "series-up" : "series-down" }],
      label: (row) => dateLabel(row.from).slice(5),
      labelCount: 9,
      tip: (event, row) =>
        showTip(event, row.kind === "win" ? "Winning streak" : "Losing streak", [
          ["Trades", num(row.count, 0)],
          ["Amount", money(row.amount), tone(row.amount)],
          ["From", dateLabel(row.from)],
          ["To", dateLabel(row.to)],
        ]),
    });
  }
  function signedRun(row) {
    const value = mode === "count" ? row.count : Math.abs(row.amount);
    return row.kind === "win" ? value : -value;
  }
  redraw();
}

function detailsTab(body, view) {
  const rows = view.details.rows;
  const built = table(
    [
      { key: "metric", label: "Metric", cell: (row) => h("td", { text: row.metric }), value: (row) => row.metric, numeric: false, sortable: false },
      { key: "all", label: "All", cell: (row) => byKind(row.all, row.kind), value: (row) => row.all, sortable: false },
      { key: "long", label: "Long", cell: (row) => byKind(row.long, row.kind), value: (row) => row.long, sortable: false },
      { key: "short", label: "Short", cell: (row) => byKind(row.short, row.kind), value: (row) => row.short, sortable: false },
    ],
    rows
  );
  body.appendChild(h("div", { class: "table-wrap" }, built.node));
  body.appendChild(
    h("p", { class: "hint muted", text: "Outliers are trades outside Tukey's fence (1.5 × the inter-quartile range) — the ones an average stops describing." })
  );
}

function excursionPanel(view) {
  const points = view.curve.points.filter((point) => point.r !== null);
  const section = panel(
    "Risk and excursion",
    "Where the stops and the exits are actually costing money"
  );
  const left = h("div", { class: "chart" });
  const right = h("div", { class: "chart" });
  const exits = h("div", { class: "chart" });
  section.body.appendChild(
    h(
      "div",
      { class: "split even" },
      h("div", {}, h("div", { class: "chart-title", text: "Adverse excursion against result" }), left),
      h("div", {}, h("div", { class: "chart-title", text: "Favourable excursion against result" }), right)
    )
  );
  section.body.appendChild(
    h("div", { class: "chart-title", style: "margin-top:12px", text: "R-multiple, trade by trade" })
  );
  section.body.appendChild(exits);
  scatterChart(left, points, { x: (point) => point.mae_r, y: (point) => point.r, xLabel: "MAE (R) — how far it went against the trade" });
  scatterChart(right, points, { x: (point) => point.mfe_r, y: (point) => point.r, xLabel: "MFE (R) — how far it went for the trade" });
  barSeries(exits, points, {
    height: 200,
    maxBar: 14,
    values: (point) => [point.r],
    parts: (point) => [{ value: point.r, class: point.r >= 0 ? "series-up" : "series-down" }],
    label: (point) => "#" + point.i,
    labelCount: 12,
    format: (value) => num(value, 1) + "R",
    tip: (event, point) =>
      showTip(event, [`#${point.i} ${point.dir}`, dateLabel(point.t)], [
        ["R", rMultiple(point.r), tone(point.r)],
        ["P&L", money(point.pnl), tone(point.pnl)],
        ["MAE / MFE", `${num(point.mae_r, 2)} / ${num(point.mfe_r, 2)}`],
        ["Exit", point.reason || NA],
      ]),
  });
  if (view.excursion.without_stop) {
    section.body.appendChild(
      h("p", {
        class: "hint muted",
        text: `${view.excursion.without_stop} trade(s) reached the market with no stop and carry no R-multiple; they are absent from both scatters.`,
      })
    );
  }
  return section.node;
}

function diagnosticsPanel(view) {
  const counts = view.diagnostics.counts || {};
  const findings = view.diagnostics.findings || [];
  const summary = ["critical", "warning", "info"]
    .filter((name) => counts[name])
    .map((name) => `${counts[name]} ${name}`)
    .join(", ");
  const section = panel("Diagnostics", summary || "every rule passed on this run");
  if (!findings.length) {
    section.body.appendChild(h("p", { class: "empty", text: "No findings. Every rule passed on this run." }));
    return section.node;
  }
  for (const finding of findings) {
    section.body.appendChild(
      h(
        "article",
        { class: "finding " + finding.severity },
        h("h3", { text: finding.title }),
        h("div", { class: "code", text: finding.code }),
        h("p", { text: finding.detail }),
        h("p", { class: "do" }, h("strong", { text: "Do this: " }), finding.suggestion),
        h("div", {
          class: "evidence",
          text: Object.entries(finding.evidence || {})
            .map(([key, value]) => `${key}=${value}`)
            .join(" · "),
        })
      )
    );
  }
  return section.node;
}

function tradesPanel(view) {
  const all = view.trades;
  const section = panel("List of trades", `${all.length} closed trade${all.length === 1 ? "" : "s"}`);
  let filter = "all";
  const search = h("input", {
    type: "search",
    placeholder: "exit reason, id…",
    style: "background:var(--panel-2);border:1px solid var(--border);color:var(--text);border-radius:8px;padding:5px 10px;font:inherit;font-size:12px",
    oninput: () => redraw(),
  });
  const controls = h(
    "div",
    { style: "display:flex;gap:8px;align-items:center;margin-left:auto" },
    segmented(
      [
        { label: "All", value: "all" },
        { label: "Long", value: "long" },
        { label: "Short", value: "short" },
        { label: "Winners", value: "win" },
        { label: "Losers", value: "loss" },
      ],
      (value) => {
        filter = value;
        redraw();
      }
    ),
    search
  );
  section.header.appendChild(controls);

  const columns = [
    { key: "index", label: "#", cell: (row) => String(row.index), value: (row) => row.index },
    {
      key: "direction",
      label: "Dir",
      cell: (row) => h("td", {}, h("span", { class: "pill " + row.direction.toLowerCase(), text: row.direction === "LONG" ? "L" : "S" })),
      value: (row) => row.direction,
    },
    { key: "opened_at", label: "Opened", cell: (row) => dateLabel(row.opened_at, true), value: (row) => ms(row.opened_at) },
    { key: "closed_at", label: "Closed", cell: (row) => dateLabel(row.closed_at, true), value: (row) => ms(row.closed_at) },
    { key: "bars_held", label: "Bars", cell: (row) => num(row.bars_held, 0), value: (row) => row.bars_held },
    { key: "entry_price", label: "Entry", cell: (row) => price(row.entry_price), value: (row) => row.entry_price },
    { key: "exit_price", label: "Exit", cell: (row) => price(row.exit_price), value: (row) => row.exit_price },
    { key: "quantity", label: "Qty", cell: (row) => num(row.quantity, 2), value: (row) => row.quantity },
    { key: "exit_reason", label: "Exit", cell: (row) => h("td", { text: row.exit_reason || NA }), value: (row) => row.exit_reason },
    {
      key: "r_multiple",
      label: "R",
      cell: (row) => h("td", { class: "num " + tone(row.r_multiple), text: rMultiple(row.r_multiple) }),
      value: (row) => row.r_multiple,
    },
    { key: "mae_r", label: "MAE", cell: (row) => num(row.mae_r, 2), value: (row) => row.mae_r },
    { key: "mfe_r", label: "MFE", cell: (row) => num(row.mfe_r, 2), value: (row) => row.mfe_r },
    { key: "fees", label: "Fees", cell: (row) => num(row.fees, 2), value: (row) => row.fees },
    {
      key: "net_pnl",
      label: "Net P&L",
      cell: (row) => h("td", { class: "num " + tone(row.net_pnl), text: money(row.net_pnl) }),
      value: (row) => row.net_pnl,
    },
  ];
  const built = table(columns, all, { sort: "index" });
  section.body.appendChild(h("div", { class: "table-wrap scroll-y" }, built.node));

  function redraw() {
    const needle = search.value.trim().toLowerCase();
    built.redraw(
      all.filter((row) => {
        if (filter === "long" && row.direction !== "LONG") return false;
        if (filter === "short" && row.direction !== "SHORT") return false;
        if (filter === "win" && !(row.net_pnl > 0)) return false;
        if (filter === "loss" && !(row.net_pnl < 0)) return false;
        if (!needle) return true;
        return `${row.exit_reason} ${row.signal_uxid} ${row.direction}`.toLowerCase().includes(needle);
      })
    );
  }
  return section.node;
}

function contextPanel(view) {
  const meta = view.meta;
  const costs = view.costs;
  const section = panel("Run and costs", "what was tested, and at what price");
  const params = Object.entries(meta.params || {});
  section.body.appendChild(
    h(
      "div",
      { class: "kv" },
      ...[
        ["Strategy class", (meta.strategy_meta || {}).class],
        ["Module", (meta.strategy_meta || {}).module],
        ["Starting equity", num(meta.starting_equity)],
        ["Default quantity", num(meta.default_quantity, 4)],
        ["Bars after warm-up", num(meta.bars_after_warmup, 0)],
        ["Data gaps", num(meta.gaps, 0)],
        ["Spread", num(costs.spread, 5)],
        ["Slippage", num(costs.slippage, 5)],
        ["Commission per unit", num(costs.commission_per_unit, 5)],
        ["Contract size", num(costs.contract_size, 2)],
        ["Round-trip cost", num(costs.round_trip_cost, 5)],
        ["Signals emitted", num((view.activity || {}).signals_emitted, 0)],
        ["Rejected entries", num((view.activity || {}).rejected_entries, 0)],
        ...params.map(([key, value]) => [`param · ${key}`, String(value)]),
      ].map(([key, value]) =>
        h("div", {}, h("span", { class: "k", text: key }), h("span", { class: "v", text: value === undefined || value === null ? NA : value }))
      )
    )
  );
  const guide = Object.entries(view.reading_guide || {});
  if (guide.length) {
    section.node.appendChild(
      h(
        "details",
        { class: "guide" },
        h("summary", { text: "Reading guide — the conventions behind these numbers" }),
        h("dl", { class: "guide-grid" }, ...guide.flatMap(([key, text]) => [h("dt", { text: key }), h("dd", { text })]))
      )
    );
  }
  return section.node;
}

/* ── Theme ───────────────────────────────────────────────────────────── */

function toggleTheme() {
  const next = document.documentElement.getAttribute("data-theme") === "light" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  try {
    localStorage.setItem("qte-theme", next);
  } catch (error) {
    /* Private windows refuse storage; the toggle still works for this visit. */
  }
}
(function restoreTheme() {
  let saved = null;
  try {
    saved = localStorage.getItem("qte-theme");
  } catch (error) {
    saved = null;
  }
  const prefersLight = window.matchMedia && window.matchMedia("(prefers-color-scheme: light)").matches;
  document.documentElement.setAttribute("data-theme", saved || (prefersLight ? "light" : "dark"));
})();

/* ── Boot ────────────────────────────────────────────────────────────── */

(function main() {
  const app = document.getElementById("app");
  add(app, [
    masthead(VIEW),
    statTiles(VIEW),
    performancePanel(VIEW),
    pricePanel(VIEW),
    performanceAnalysis(VIEW),
    tradesAnalysis(VIEW),
    VIEW.curve.points.length ? excursionPanel(VIEW) : null,
    diagnosticsPanel(VIEW),
    VIEW.trades.length ? tradesPanel(VIEW) : null,
    contextPanel(VIEW),
    h("footer", { class: "foot" },
      h("span", { text: `qte-backtest · report schema ${VIEW.meta.schema_version}` }),
      h("span", { text: "Every figure here is derived from the JSON this page was rendered from." })
    ),
  ]);
})();
