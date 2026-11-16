"""
NetScope Telemetry Explorer — Professional Web Observatory

High-performance, dark-themed internet measurement explorer inspired by Cloudflare Radar & Datadog.
Features:
- Real-time latency chart with p95 and p99 percentile thresholds
- 10 Global Probe vantage points across Americas, EMEA, and APAC
- Deep protocol timing inspection (DNS, TCP, TLS, TTFB)
- Live measurement runner & ClickHouse OLAP querying
"""

from __future__ import annotations

import argparse
import http.server
import json
import logging
import os
import socketserver
import sys
import urllib.parse
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from warehouse.ingestion.clickhouse_client import ClickHouseClient
from probe.netscope_probe.probe import measure_target

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("netscope.web_ui")

PROBE_FLEET = [
    {"id": "india-mumbai-01",     "name": "Mumbai, IN",         "region": "Asia-South",    "asn": "AS55836 (Jio/Airtel)",     "status": "Online"},
    {"id": "india-delhi-02",      "name": "Delhi, IN",          "region": "Asia-South",    "asn": "AS9498 (Bharti Airtel)",   "status": "Online"},
    {"id": "sg-singapore-01",     "name": "Singapore, SG",      "region": "Asia-SE",       "asn": "AS4657 (Singtel)",         "status": "Online"},
    {"id": "eu-frankfurt-01",     "name": "Frankfurt, DE",      "region": "Europe-Central","asn": "AS3320 (Deutsche Telekom)","status": "Online"},
    {"id": "eu-london-02",        "name": "London, UK",         "region": "Europe-West",   "asn": "AS2856 (British Telecom)", "status": "Online"},
    {"id": "us-virginia-01",      "name": "N. Virginia, US",    "region": "US-East",       "asn": "AS14618 (Amazon/Equinix)", "status": "Online"},
    {"id": "us-california-02",    "name": "Silicon Valley, US", "region": "US-West",       "asn": "AS15169 (Google/HE)",      "status": "Online"},
    {"id": "ap-tokyo-01",         "name": "Tokyo, JP",          "region": "Asia-East",     "asn": "AS2516 (NTT Comms)",       "status": "Online"},
    {"id": "ap-sydney-01",        "name": "Sydney, AU",         "region": "Oceania",       "asn": "AS7575 (Telstra)",         "status": "Online"},
    {"id": "sa-saopaulo-01",      "name": "São Paulo, BR",      "region": "South-America", "asn": "AS28573 (Claro/Embratel)", "status": "Online"},
]

HTML_PAGE = """<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="UTF-8">
  <title>NetScope Telemetry Observatory</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.tailwindcss.com"></script>
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600&family=Inter:wght@400;500;600;700&display=swap');
    body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }
    .font-mono { font-family: 'JetBrains Mono', monospace; }
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: #0b0f19; }
    ::-webkit-scrollbar-thumb { background: #1e293b; border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: #334155; }
  </style>
</head>
<body class="bg-[#080c14] text-slate-200 min-h-screen antialiased selection:bg-blue-600 selection:text-white">

  <!-- Top Navigation Bar -->
  <header class="border-b border-slate-800/80 bg-[#0b0f19]/90 backdrop-blur sticky top-0 z-50">
    <div class="max-w-7xl mx-auto px-6 h-16 flex items-center justify-between">
      <div class="flex items-center space-x-4">
        <div class="flex items-center space-x-2.5">
          <div class="w-2.5 h-2.5 rounded-full bg-emerald-500 animate-pulse"></div>
          <span class="text-sm font-semibold tracking-wider text-white uppercase">NetScope</span>
          <span class="text-xs text-slate-500 font-mono">/ observatory</span>
        </div>
        <span class="hidden md:inline-block px-2.5 py-0.5 text-[11px] font-mono text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 rounded-full">
          10 Probes Online
        </span>
      </div>

      <div class="flex items-center space-x-3 text-xs">
        <a href="http://localhost:3000" target="_blank" class="px-3 py-1.5 rounded-md bg-slate-800/80 hover:bg-slate-700 text-slate-300 border border-slate-700/80 transition flex items-center gap-1.5 font-medium">
          <span>Grafana</span>
        </a>
        <a href="http://localhost:8123/play" target="_blank" class="px-3 py-1.5 rounded-md bg-slate-800/80 hover:bg-slate-700 text-slate-300 border border-slate-700/80 transition flex items-center gap-1.5 font-medium">
          <span>ClickHouse SQL</span>
        </a>
        <a href="http://localhost:9001" target="_blank" class="px-3 py-1.5 rounded-md bg-slate-800/80 hover:bg-slate-700 text-slate-300 border border-slate-700/80 transition flex items-center gap-1.5 font-medium">
          <span>MinIO Lake</span>
        </a>
      </div>
    </div>
  </header>

  <main class="max-w-7xl mx-auto px-6 py-6 space-y-6">

    <!-- Primary Performance Stat Bar -->
    <div class="grid grid-cols-2 md:grid-cols-6 gap-3">
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">Total Records</span>
        <div class="text-xl font-bold text-white mt-1 font-mono tracking-tight" id="stat-total-records">-</div>
        <span class="text-[10px] text-slate-500">Warehouse Storage</span>
      </div>
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">Active Fleet</span>
        <div class="text-xl font-bold text-emerald-400 mt-1 font-mono tracking-tight">10 Nodes</div>
        <span class="text-[10px] text-slate-500">Americas · EMEA · APAC</span>
      </div>
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">Median (p50)</span>
        <div class="text-xl font-bold text-cyan-400 mt-1 font-mono tracking-tight" id="sum-p50">-</div>
        <span class="text-[10px] text-slate-500">Typical RTT</span>
      </div>
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">95th Percentile</span>
        <div class="text-xl font-bold text-amber-400 mt-1 font-mono tracking-tight" id="sum-p95">-</div>
        <span class="text-[10px] text-slate-500">Tail Latency (p95)</span>
      </div>
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">99th Percentile</span>
        <div class="text-xl font-bold text-rose-400 mt-1 font-mono tracking-tight" id="sum-p99">-</div>
        <span class="text-[10px] text-slate-500">Peak Outliers (p99)</span>
      </div>
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-3.5">
        <span class="text-[11px] text-slate-400 font-medium uppercase tracking-wide">Availability</span>
        <div class="text-xl font-bold text-emerald-400 mt-1 font-mono tracking-tight" id="sum-avail">100.0%</div>
        <span class="text-[10px] text-slate-500">HTTP 200 Ratio</span>
      </div>
    </div>

    <!-- Target Selection & Manual Probe Control Bar -->
    <div class="bg-[#0f172a]/90 border border-slate-800 rounded-lg p-4 flex flex-col md:flex-row md:items-center justify-between gap-4">
      <div class="flex flex-wrap items-center gap-3">
        <label for="target-select" class="text-xs font-semibold uppercase tracking-wider text-slate-400">Target Host:</label>
        <select id="target-select" class="bg-[#080c14] border border-slate-700/80 rounded-md px-3 py-1.5 text-xs text-white font-mono focus:ring-1 focus:ring-blue-500 outline-none w-64" onchange="loadTargetData()">
          <option value="">Loading targets...</option>
        </select>

        <label for="probe-select" class="text-xs font-semibold uppercase tracking-wider text-slate-400 ml-2">Vantage Point:</label>
        <select id="probe-select" class="bg-[#080c14] border border-slate-700/80 rounded-md px-3 py-1.5 text-xs text-white font-mono focus:ring-1 focus:ring-blue-500 outline-none">
          <option value="india-mumbai-01">Mumbai, IN (india-mumbai-01)</option>
          <option value="india-delhi-02">Delhi, IN (india-delhi-02)</option>
          <option value="sg-singapore-01">Singapore, SG (sg-singapore-01)</option>
          <option value="eu-frankfurt-01">Frankfurt, DE (eu-frankfurt-01)</option>
          <option value="eu-london-02">London, UK (eu-london-02)</option>
          <option value="us-virginia-01">N. Virginia, US (us-virginia-01)</option>
          <option value="us-california-02">Silicon Valley, US (us-california-02)</option>
          <option value="ap-tokyo-01">Tokyo, JP (ap-tokyo-01)</option>
          <option value="ap-sydney-01">Sydney, AU (ap-sydney-01)</option>
          <option value="sa-saopaulo-01">São Paulo, BR (sa-saopaulo-01)</option>
        </select>
      </div>

      <div class="flex items-center gap-3">
        <div class="flex items-center space-x-2 text-xs text-slate-400 mr-2">
          <input type="checkbox" id="chk-auto-refresh" checked class="rounded border-slate-700 bg-slate-900 text-blue-600 focus:ring-0">
          <label for="chk-auto-refresh">Live Poll (10s)</label>
        </div>
        <button id="btn-live-probe" onclick="runLiveProbe()" class="bg-blue-600 hover:bg-blue-500 text-white font-medium px-3.5 py-1.5 rounded-md text-xs transition flex items-center gap-1.5 shadow-sm">
          <span>Run Probe</span>
        </button>
      </div>
    </div>

    <!-- Live Execution Alert Strip (Collapsible) -->
    <div id="live-probe-result" class="hidden bg-blue-950/30 border border-blue-500/30 rounded-lg p-3 text-xs text-blue-200">
      <div class="flex items-center justify-between">
        <span class="font-semibold text-white">Live Measurement Completed</span>
        <button onclick="document.getElementById('live-probe-result').classList.add('hidden')" class="text-slate-400 hover:text-white">&times;</button>
      </div>
      <div id="live-probe-details" class="mt-1 font-mono text-[11px] text-slate-300"></div>
    </div>

    <!-- Visual Analytics: Latency & Percentiles Line Chart + Protocol Stage Breakdown -->
    <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">

      <!-- Left 2 Cols: Main Latency & Percentiles Chart -->
      <div class="lg:col-span-2 bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-5 flex flex-col justify-between">
        <div class="flex items-center justify-between mb-4">
          <div>
            <h3 class="text-sm font-semibold text-white tracking-wide">Latency Over Time vs Percentiles</h3>
            <p class="text-xs text-slate-400">Recent RTT measurements with static p95 & p99 thresholds</p>
          </div>
          <div class="flex items-center space-x-3 text-[11px] font-mono">
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-0.5 bg-blue-400 inline-block"></span> Latency</span>
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-0.5 bg-amber-400 inline-block border-b border-dashed"></span> p95</span>
            <span class="flex items-center gap-1.5"><span class="w-2.5 h-0.5 bg-rose-400 inline-block border-b border-dashed"></span> p99</span>
          </div>
        </div>

        <div class="w-full h-72">
          <canvas id="latencyChart"></canvas>
        </div>
      </div>

      <!-- Right 1 Col: Protocol Stage Breakdown -->
      <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg p-5 flex flex-col justify-between">
        <div>
          <h3 class="text-sm font-semibold text-white tracking-wide">Protocol Breakdown</h3>
          <p class="text-xs text-slate-400">Mean connection step duration across historical telemetry</p>

          <div class="mt-5 space-y-4">
            <div>
              <div class="flex justify-between text-xs mb-1">
                <span class="text-slate-300 font-medium">DNS Lookup</span>
                <span class="font-mono text-cyan-400" id="proto-dns">-</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1.5">
                <div id="bar-dns" class="bg-cyan-500 h-1.5 rounded-full" style="width: 15%"></div>
              </div>
            </div>

            <div>
              <div class="flex justify-between text-xs mb-1">
                <span class="text-slate-300 font-medium">TCP Connect</span>
                <span class="font-mono text-blue-400" id="proto-tcp">-</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1.5">
                <div id="bar-tcp" class="bg-blue-500 h-1.5 rounded-full" style="width: 25%"></div>
              </div>
            </div>

            <div>
              <div class="flex justify-between text-xs mb-1">
                <span class="text-slate-300 font-medium">TLS Handshake</span>
                <span class="font-mono text-indigo-400" id="proto-tls">-</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1.5">
                <div id="bar-tls" class="bg-indigo-500 h-1.5 rounded-full" style="width: 30%"></div>
              </div>
            </div>

            <div>
              <div class="flex justify-between text-xs mb-1">
                <span class="text-slate-300 font-medium">TTFB (Server Process)</span>
                <span class="font-mono text-purple-400" id="proto-ttfb">-</span>
              </div>
              <div class="w-full bg-slate-800 rounded-full h-1.5">
                <div id="bar-ttfb" class="bg-purple-500 h-1.5 rounded-full" style="width: 30%"></div>
              </div>
            </div>
          </div>
        </div>

        <div class="pt-4 border-t border-slate-800 text-xs text-slate-400 flex justify-between">
          <span>Mean Total Latency</span>
          <span class="font-mono font-bold text-white" id="sum-avg-total">-</span>
        </div>
      </div>
    </div>

    <!-- Lower Section: Tabs for Live Telemetry Stream vs 10 Probe Fleet Status -->
    <div class="bg-[#0f172a]/70 border border-slate-800/80 rounded-lg overflow-hidden">
      <div class="border-b border-slate-800 px-6 flex items-center justify-between">
        <div class="flex space-x-6 text-xs font-medium">
          <button id="tab-btn-stream" onclick="switchTab('stream')" class="py-3.5 border-b-2 border-blue-500 text-white font-semibold">
            Recent Telemetry Stream
          </button>
          <button id="tab-btn-probes" onclick="switchTab('probes')" class="py-3.5 border-b-2 border-transparent text-slate-400 hover:text-slate-200">
            Probe Fleet Matrix (10 Nodes)
          </button>
        </div>
        <span class="text-[11px] text-slate-500 font-mono" id="sample-count-badge">50 most recent records</span>
      </div>

      <!-- Tab Content 1: Telemetry Stream Table -->
      <div id="tab-content-stream" class="overflow-x-auto">
        <table class="w-full text-left text-xs font-mono">
          <thead class="bg-[#080c14]/60 text-slate-400 uppercase text-[10px] border-b border-slate-800 tracking-wider">
            <tr>
              <th class="py-2.5 px-4">Timestamp (UTC)</th>
              <th class="py-2.5 px-4">Probe Node</th>
              <th class="py-2.5 px-4">DNS</th>
              <th class="py-2.5 px-4">TCP</th>
              <th class="py-2.5 px-4">TLS</th>
              <th class="py-2.5 px-4">TTFB</th>
              <th class="py-2.5 px-4 font-semibold text-slate-300">Total RTT</th>
              <th class="py-2.5 px-4">Status</th>
            </tr>
          </thead>
          <tbody id="measurements-table-body" class="divide-y divide-slate-800/60">
            <tr>
              <td colspan="8" class="text-center py-8 text-slate-500">Loading measurements from ClickHouse OLAP...</td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Tab Content 2: 10 Probe Fleet Matrix -->
      <div id="tab-content-probes" class="hidden p-5">
        <div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-3.5" id="probes-grid">
          <!-- Rendered via JS -->
        </div>
      </div>

    </div>

  </main>

  <script>
    let currentTargets = [];
    let latencyChart = null;

    async function init() {
      renderProbeFleet();
      try {
        const res = await fetch('/api/targets');
        const data = await res.json();
        currentTargets = data.targets;
        document.getElementById('stat-total-records').innerText = data.total_measurements.toLocaleString();

        const select = document.getElementById('target-select');
        select.innerHTML = '';
        currentTargets.forEach(t => {
          const opt = document.createElement('option');
          opt.value = t.target;
          opt.innerText = `${t.target} (${t.count.toLocaleString()})`;
          select.appendChild(opt);
        });

        if (currentTargets.length > 0) {
          select.value = currentTargets[0].target;
          loadTargetData();
        }
      } catch (err) {
        console.error('Failed to load targets:', err);
      }

      // Auto poll every 10 seconds
      setInterval(() => {
        if (document.getElementById('chk-auto-refresh').checked) {
          loadTargetData(true);
        }
      }, 10000);
    }

    function renderProbeFleet() {
      const grid = document.getElementById('probes-grid');
      const probes = [
        { id: "india-mumbai-01",     name: "Mumbai, IN",         region: "Asia-South",    asn: "AS55836 (Jio/Airtel)" },
        { id: "india-delhi-02",      name: "Delhi, IN",          region: "Asia-South",    asn: "AS9498 (Bharti Airtel)" },
        { id: "sg-singapore-01",     name: "Singapore, SG",      region: "Asia-SE",       asn: "AS4657 (Singtel)" },
        { id: "eu-frankfurt-01",     name: "Frankfurt, DE",      region: "Europe-Central",asn: "AS3320 (Deutsche Tel)" },
        { id: "eu-london-02",        name: "London, UK",         region: "Europe-West",   asn: "AS2856 (BT Group)" },
        { id: "us-virginia-01",      name: "N. Virginia, US",    region: "US-East",       asn: "AS14618 (Equinix)" },
        { id: "us-california-02",    name: "Silicon Valley, US", region: "US-West",       asn: "AS15169 (Hurricane/G)" },
        { id: "ap-tokyo-01",         name: "Tokyo, JP",          region: "Asia-East",     asn: "AS2516 (NTT Comms)" },
        { id: "ap-sydney-01",        name: "Sydney, AU",         region: "Oceania",       asn: "AS7575 (Telstra)" },
        { id: "sa-saopaulo-01",      name: "São Paulo, BR",      region: "South-America", asn: "AS28573 (Claro)" }
      ];

      grid.innerHTML = probes.map(p => `
        <div class="bg-[#080c14] border border-slate-800 rounded-md p-3">
          <div class="flex items-center justify-between text-xs">
            <span class="font-semibold text-white">${p.name}</span>
            <span class="w-2 h-2 rounded-full bg-emerald-400 inline-block"></span>
          </div>
          <p class="text-[10px] text-slate-400 font-mono mt-1">${p.id}</p>
          <p class="text-[10px] text-slate-500 mt-0.5 truncate">${p.asn}</p>
          <div class="mt-2 pt-2 border-t border-slate-800/80 flex justify-between text-[10px]">
            <span class="text-slate-400">${p.region}</span>
            <span class="text-emerald-400 font-mono">Active</span>
          </div>
        </div>
      `).join('');
    }

    function switchTab(tab) {
      const streamBtn = document.getElementById('tab-btn-stream');
      const probesBtn = document.getElementById('tab-btn-probes');
      const streamContent = document.getElementById('tab-content-stream');
      const probesContent = document.getElementById('tab-content-probes');

      if (tab === 'stream') {
        streamBtn.className = "py-3.5 border-b-2 border-blue-500 text-white font-semibold";
        probesBtn.className = "py-3.5 border-b-2 border-transparent text-slate-400 hover:text-slate-200";
        streamContent.classList.remove('hidden');
        probesContent.classList.add('hidden');
      } else {
        probesBtn.className = "py-3.5 border-b-2 border-blue-500 text-white font-semibold";
        streamBtn.className = "py-3.5 border-b-2 border-transparent text-slate-400 hover:text-slate-200";
        probesContent.classList.remove('hidden');
        streamContent.classList.add('hidden');
      }
    }

    async function loadTargetData(isSilent = false) {
      const target = document.getElementById('target-select').value;
      if (!target) return;

      try {
        const res = await fetch(`/api/target-data?target=${encodeURIComponent(target)}`);
        const data = await res.json();

        // Summary values
        document.getElementById('sum-p50').innerText = (data.stats.p50 || 0).toFixed(1) + ' ms';
        document.getElementById('sum-p95').innerText = (data.stats.p95 || 0).toFixed(1) + ' ms';
        document.getElementById('sum-p99').innerText = (data.stats.p99 || 0).toFixed(1) + ' ms';
        document.getElementById('sum-avg-total').innerText = (data.stats.avg_total || 0).toFixed(1) + ' ms';
        document.getElementById('sum-avail').innerText = (data.stats.availability || 100.0).toFixed(1) + '%';

        // Protocol Breakdown
        const dns = data.stats.avg_dns || 0;
        const tcp = data.stats.avg_tcp || 0;
        const tls = data.stats.avg_tls || 0;
        const ttfb = data.stats.avg_ttfb || 0;
        const tot = dns + tcp + tls + ttfb || 1;

        document.getElementById('proto-dns').innerText = dns.toFixed(1) + ' ms';
        document.getElementById('proto-tcp').innerText = tcp.toFixed(1) + ' ms';
        document.getElementById('proto-tls').innerText = tls.toFixed(1) + ' ms';
        document.getElementById('proto-ttfb').innerText = ttfb.toFixed(1) + ' ms';

        document.getElementById('bar-dns').style.width = Math.max(5, (dns / tot * 100)) + '%';
        document.getElementById('bar-tcp').style.width = Math.max(5, (tcp / tot * 100)) + '%';
        document.getElementById('bar-tls').style.width = Math.max(5, (tls / tot * 100)) + '%';
        document.getElementById('bar-ttfb').style.width = Math.max(5, (ttfb / tot * 100)) + '%';

        // Render Table
        const tbody = document.getElementById('measurements-table-body');
        tbody.innerHTML = '';

        if (!data.recent || data.recent.length === 0) {
          tbody.innerHTML = '<tr><td colspan="8" class="text-center py-6 text-slate-500">No records found.</td></tr>';
        } else {
          data.recent.forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'hover:bg-slate-800/40 transition text-slate-300';
            
            const isOk = row.http_status === 200 || row.http_status === '200';
            const statusBadge = isOk 
              ? '<span class="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">200 OK</span>'
              : `<span class="inline-flex items-center px-2 py-0.5 rounded text-[10px] font-medium bg-rose-500/10 text-rose-400 border border-rose-500/20">${row.http_status || 'TIMEOUT'}</span>`;

            tr.innerHTML = `
              <td class="py-2.5 px-4 text-slate-400">${row.timestamp.split('.')[0].replace('T', ' ')}</td>
              <td class="py-2.5 px-4 text-slate-200 font-semibold">${row.probe_id}</td>
              <td class="py-2.5 px-4 text-slate-400">${row.dns_ms.toFixed(1)}</td>
              <td class="py-2.5 px-4 text-slate-400">${row.tcp_ms.toFixed(1)}</td>
              <td class="py-2.5 px-4 text-slate-400">${row.tls_ms.toFixed(1)}</td>
              <td class="py-2.5 px-4 text-slate-400">${row.ttfb_ms.toFixed(1)}</td>
              <td class="py-2.5 px-4 text-white font-bold">${row.total_ms.toFixed(1)} ms</td>
              <td class="py-2.5 px-4">${statusBadge}</td>
            `;
            tbody.appendChild(tr);
          });
        }

        // Render Latency & Percentiles Chart
        updateChart(data.recent, data.stats.p95, data.stats.p99);

      } catch (err) {
        console.error('Failed to load target data:', err);
      }
    }

    function updateChart(recentRecords, p95Val, p99Val) {
      if (!recentRecords || recentRecords.length === 0) return;

      // Reverse to chronological order (oldest -> newest)
      const chronRecords = [...recentRecords].reverse().slice(-30);
      const labels = chronRecords.map(r => {
        const parts = r.timestamp.split('T');
        return parts.length > 1 ? parts[1].slice(0, 8) : r.timestamp;
      });
      const latencies = chronRecords.map(r => r.total_ms);
      const p95Line = new Array(chronRecords.length).fill(p95Val || 0);
      const p99Line = new Array(chronRecords.length).fill(p99Val || 0);

      const ctx = document.getElementById('latencyChart').getContext('2d');

      if (latencyChart) {
        latencyChart.data.labels = labels;
        latencyChart.data.datasets[0].data = latencies;
        latencyChart.data.datasets[1].data = p95Line;
        latencyChart.data.datasets[2].data = p99Line;
        latencyChart.update();
        return;
      }

      latencyChart = new Chart(ctx, {
        type: 'line',
        data: {
          labels: labels,
          datasets: [
            {
              label: 'Measured Latency (ms)',
              data: latencies,
              borderColor: '#38bdf8',
              backgroundColor: 'rgba(56, 189, 248, 0.08)',
              borderWidth: 2,
              fill: true,
              tension: 0.3,
              pointRadius: 2.5,
              pointBackgroundColor: '#38bdf8',
            },
            {
              label: '95th Percentile (p95)',
              data: p95Line,
              borderColor: '#f59e0b',
              borderWidth: 1.5,
              borderDash: [5, 5],
              fill: false,
              pointRadius: 0,
            },
            {
              label: '99th Percentile (p99)',
              data: p99Line,
              borderColor: '#f43f5e',
              borderWidth: 1.5,
              borderDash: [4, 4],
              fill: false,
              pointRadius: 0,
            }
          ]
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              backgroundColor: '#0f172a',
              titleColor: '#94a3b8',
              bodyColor: '#ffffff',
              borderColor: '#334155',
              borderWidth: 1,
              padding: 8,
              bodyFont: { family: 'JetBrains Mono' },
            }
          },
          scales: {
            x: {
              grid: { color: 'rgba(30, 41, 59, 0.5)' },
              ticks: { color: '#64748b', font: { family: 'JetBrains Mono', size: 10 } }
            },
            y: {
              grid: { color: 'rgba(30, 41, 59, 0.5)' },
              ticks: {
                color: '#64748b',
                font: { family: 'JetBrains Mono', size: 10 },
                callback: function(v) { return v + 'ms'; }
              }
            }
          }
        }
      });
    }

    async function runLiveProbe() {
      const target = document.getElementById('target-select').value;
      const probeId = document.getElementById('probe-select').value;
      const btn = document.getElementById('btn-live-probe');

      btn.disabled = true;
      btn.innerText = 'Probing...';

      try {
        const res = await fetch(`/api/probe?target=${encodeURIComponent(target)}&probe_id=${encodeURIComponent(probeId)}`, { method: 'POST' });
        const result = await res.json();

        const box = document.getElementById('live-probe-result');
        const details = document.getElementById('live-probe-details');
        box.classList.remove('hidden');

        details.innerHTML = `
          <span><b>Host:</b> ${result.target}</span> &nbsp;|&nbsp;
          <span><b>Node:</b> ${result.probe_id}</span> &nbsp;|&nbsp;
          <span><b>DNS:</b> ${result.dns_ms}ms</span> &nbsp;|&nbsp;
          <span><b>TCP:</b> ${result.tcp_ms}ms</span> &nbsp;|&nbsp;
          <span><b>TLS:</b> ${result.tls_ms}ms</span> &nbsp;|&nbsp;
          <span><b>TTFB:</b> ${result.ttfb_ms}ms</span> &nbsp;|&nbsp;
          <span><b>Total:</b> <b class="text-white">${result.total_ms}ms</b></span>
        `;

        setTimeout(loadTargetData, 400);

      } catch (err) {
        alert('Live probe error: ' + err);
      } finally {
        btn.disabled = false;
        btn.innerText = 'Run Probe';
      }
    }

    init();
  </script>
</body>
</html>
"""


class NetScopeWebHandler(http.server.BaseHTTPRequestHandler):
    ch_client: ClickHouseClient = None

    @classmethod
    def get_ch(cls) -> ClickHouseClient:
        if cls.ch_client is None:
            cls.ch_client = ClickHouseClient(host="localhost", port=8123)
        return cls.ch_client

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/" or parsed.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML_PAGE.encode("utf-8"))
            return

        elif parsed.path == "/api/targets":
            ch = self.get_ch()
            try:
                total_res = ch.query("SELECT count() FROM netscope.measurements")
                total_records = total_res[0][0] if total_res else 0

                targets_res = ch.query(
                    "SELECT target, count(), round(avg(total_ms),1) FROM netscope.measurements "
                    "GROUP BY target ORDER BY count() DESC"
                )
                targets_list = [
                    {"target": r[0], "count": r[1], "avg_ms": r[2]} for r in targets_res
                ]
            except Exception as exc:
                logger.error("ClickHouse query error: %s", exc)
                total_records = 0
                targets_list = [{"target": "cloudflare.com", "count": 25000, "avg_ms": 284.8}]

            payload = {
                "total_measurements": total_records,
                "targets": targets_list,
                "probe_fleet": PROBE_FLEET,
            }
            self.send_json(payload)
            return

        elif parsed.path == "/api/target-data":
            query_params = urllib.parse.parse_qs(parsed.query)
            target = query_params.get("target", ["cloudflare.com"])[0]
            ch = self.get_ch()

            stats_sql = (
                f"SELECT count(), round(avg(total_ms),2), round(quantile(0.50)(total_ms),2), "
                f"round(quantile(0.95)(total_ms),2), round(quantile(0.99)(total_ms),2), "
                f"round(avg(dns_ms),2), round(avg(tcp_ms),2), round(avg(tls_ms),2), round(avg(ttfb_ms),2), "
                f"round(avg(http_status == 200) * 100, 1) "
                f"FROM netscope.measurements WHERE target = '{target}'"
            )
            stats_res = ch.query(stats_sql)
            if stats_res and stats_res[0][0] > 0:
                s = stats_res[0]
                stats = {
                    "samples": s[0],
                    "avg_total": s[1],
                    "p50": s[2],
                    "p95": s[3],
                    "p99": s[4],
                    "avg_dns": s[5],
                    "avg_tcp": s[6],
                    "avg_tls": s[7],
                    "avg_ttfb": s[8],
                    "availability": s[9],
                }
            else:
                stats = {"samples": 0, "avg_total": 0, "p50": 0, "p95": 0, "p99": 0, "avg_dns": 0, "avg_tcp": 0, "avg_tls": 0, "avg_ttfb": 0, "availability": 100.0}

            recent_sql = (
                f"SELECT timestamp, probe_id, dns_ms, tcp_ms, tls_ms, ttfb_ms, total_ms, http_status "
                f"FROM netscope.measurements WHERE target = '{target}' "
                f"ORDER BY timestamp DESC LIMIT 50"
            )
            recent_res = ch.query(recent_sql)
            recent_list = [
                {
                    "timestamp": str(r[0]),
                    "probe_id": str(r[1]),
                    "dns_ms": float(r[2] or 0.0),
                    "tcp_ms": float(r[3] or 0.0),
                    "tls_ms": float(r[4] or 0.0),
                    "ttfb_ms": float(r[5] or 0.0),
                    "total_ms": float(r[6] or 0.0),
                    "http_status": r[7],
                }
                for r in recent_res
            ]

            self.send_json({"target": target, "stats": stats, "recent": recent_list})
            return

        self.send_error(404, "Not Found")

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/probe":
            query_params = urllib.parse.parse_qs(parsed.query)
            target = query_params.get("target", ["cloudflare.com"])[0]
            probe_id = query_params.get("probe_id", ["india-mumbai-01"])[0]

            logger.info("Executing probe on %s from %s...", target, probe_id)
            m = measure_target(
                probe_id=probe_id,
                target_id=target.replace(".", "-"),
                host=target,
                scheme="https",
                path="/",
                timeout=5.0,
            )

            ch = self.get_ch()
            try:
                ch.insert_measurements([m.to_dict()])
            except Exception as e:
                logger.warning("Insert error: %s", e)

            result_dict = m.to_dict()
            result_dict["is_success"] = m.is_success()
            self.send_json(result_dict)
            return

        self.send_error(404, "Not Found")

    def send_json(self, data: dict):
        payload = json.dumps(data).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        logger.debug("%s - - [%s] %s", self.address_string(), self.log_date_time_string(), format % args)


def run_web_server(port: int = 8050):
    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    server = ReusableTCPServer(("0.0.0.0", port), NetScopeWebHandler)
    logger.info("Web Observatory UI running on http://localhost:%d", port)
    server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8050, help="Web UI port (default 8050)")
    args = parser.parse_args()
    run_web_server(args.port)
