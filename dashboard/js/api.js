/* ==========================================================================
   MLB Dashboard — API Data Fetching & Sync
   ========================================================================== */

import { state } from './state.js';

const listeners = [];

export function onDataReceived(fn) {
    listeners.push(fn);
}

function notifyDataReceived() {
    for (const fn of listeners) {
        try {
            fn();
        } catch (err) {
            console.error('Error in onDataReceived listener:', err);
        }
    }
}

export async function fetchData() {
    if (state.isFetching) return;
    state.isFetching = true;

    const btnRefresh = document.getElementById('btn-refresh');
    if (btnRefresh) btnRefresh.classList.add('spinning');

    try {
        const [topRes, beRes, metRes] = await Promise.all([
            fetch('/topology'),
            fetch('/backends'),
            fetch('/proxy_metrics')
        ]);

        if (!topRes.ok || !beRes.ok || !metRes.ok) {
            throw new Error('Erreur HTTP lors de la récupération des métriques');
        }

        state.topology = await topRes.json();
        state.backends = await beRes.json();
        state.proxyMetrics = await metRes.json();

        const now = Date.now();
        state.lastRefreshTime = now;
        state.fetchError = false;

        // Compute instantaneous throughput rates
        if (state.prevMetricsTime && state.prevProxyMetrics) {
            const dt = (now - state.prevMetricsTime) / 1000;
            if (dt >= 0.1) {
                state.proxyRates = {};
                let sumRateIn = 0;
                let sumRateOut = 0;
                for (const proxyId in state.proxyMetrics) {
                    const curr = state.proxyMetrics[proxyId] || {};
                    const prev = state.prevProxyMetrics[proxyId];
                    if (!prev) {
                        state.proxyRates[proxyId] = { rate_in: 0, rate_out: 0 };
                        continue;
                    }
                    const dIn = Math.max(0, (curr.bytes_in || 0) - (prev.bytes_in || 0));
                    const dOut = Math.max(0, (curr.bytes_out || 0) - (prev.bytes_out || 0));
                    const rIn = dIn / dt;
                    const rOut = dOut / dt;
                    state.proxyRates[proxyId] = { rate_in: rIn, rate_out: rOut };
                    sumRateIn += rIn;
                    sumRateOut += rOut;
                }
                state.totalRateIn = sumRateIn;
                state.totalRateOut = sumRateOut;
            }
        } else {
            state.proxyRates = {};
            state.totalRateIn = 0;
            state.totalRateOut = 0;
        }
        state.prevProxyMetrics = JSON.parse(JSON.stringify(state.proxyMetrics));
        state.prevMetricsTime = now;

        const banner = document.getElementById('reconnecting-banner');
        if (banner) banner.style.display = 'none';

        // Update total count on backends tab
        updateTabCounters();

        // Notify view renderers
        notifyDataReceived();

    } catch (err) {
        console.warn('Échec de synchronisation avec MLB:', err);
        state.fetchError = true;
        const banner = document.getElementById('reconnecting-banner');
        if (banner) banner.style.display = 'block';
    } finally {
        state.isFetching = false;
        if (btnRefresh) btnRefresh.classList.remove('spinning');
        updateRefreshText();
    }
}

export function updateTabCounters() {
    const uniqueAddrs = new Set();
    for (const p in state.backends) {
        const list = state.backends[p] || [];
        list.forEach(b => {
            if (b && b.address) uniqueAddrs.add(b.address);
        });
    }
    const countEl = document.getElementById('tab-backends-count');
    if (countEl) countEl.textContent = uniqueAddrs.size;
}

export function updateRefreshText() {
    const el = document.getElementById('refresh-status-text');
    if (!el) return;

    if (state.fetchError) {
        el.textContent = 'Erreur réseau';
        el.style.color = 'var(--danger)';
        return;
    }

    if (!state.lastRefreshTime) {
        el.textContent = 'Jamais';
        el.style.color = 'var(--text-faint)';
        return;
    }

    el.style.color = 'var(--text-faint)';
    const diffSec = Math.floor((Date.now() - state.lastRefreshTime) / 1000);
    if (diffSec < 3) el.textContent = 'à l\'instant';
    else el.textContent = `il y a ${diffSec}s`;
}

export function manualRefresh() {
    fetchData();
}

export function onAutoRefreshChange(val) {
    state.autoRefreshSec = parseInt(val, 10);
    setupAutoRefreshTimer();
}

export function setupAutoRefreshTimer() {
    if (state.autoRefreshTimer) {
        clearInterval(state.autoRefreshTimer);
        state.autoRefreshTimer = null;
    }

    if (state.autoRefreshSec > 0) {
        state.autoRefreshTimer = setInterval(() => {
            if (!state.isFetching) {
                fetchData();
            }
        }, state.autoRefreshSec * 1000);
    }
}
