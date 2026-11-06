/* ==========================================================================
   MLB Dashboard — Global Backends & Proxy Metrics Views
   ========================================================================== */

import {
    state,
    formatBytes,
    formatRate,
    formatNumber,
    escapeHtml,
    getNodeSources,
    evaluateBackendStatus,
    renderMetaBadges
} from './state.js';

export function onGlobalBackendSearch(query) {
    state.globalSearchQuery = query.toLowerCase().trim();
    renderBackendsView();
}

export function setCategoryFilter(cat, btn) {
    state.categoryFilter = cat;
    document.querySelectorAll('.filter-pills-group .pill-btn').forEach(b => b.classList.remove('active'));
    if (btn) btn.classList.add('active');
    renderBackendsView();
}

export function renderBackendsView() {
    const container = document.getElementById('global-backends-list');
    if (!container) return;

    const providerIds = Object.keys(state.backends).sort();
    if (providerIds.length === 0) {
        container.innerHTML = '<div class="empty-placeholder">Aucun fournisseur de backends actif</div>';
        return;
    }

    let html = '';

    providerIds.forEach(pId => {
        const node = state.topology.find(n => n.id === pId);
        const category = node ? node.category : 'backends_inventory';

        // Check category filter
        if (state.categoryFilter !== 'all' && category !== state.categoryFilter) {
            return;
        }

        let list = [...(state.backends[pId] || [])]
            .filter(b => b != null)
            .sort((a, b) => (a.address || '').localeCompare(b.address || ''));

        // Filter by search query
        if (state.globalSearchQuery) {
            list = list.filter(b => {
                const addr = (b.address || '').toLowerCase();
                if (addr.includes(state.globalSearchQuery)) return true;
                if (pId.toLowerCase().includes(state.globalSearchQuery)) return true;
                if (b.meta && JSON.stringify(b.meta).toLowerCase().includes(state.globalSearchQuery)) return true;
                return false;
            });
        }

        if (list.length === 0) return;

        const catBadgeClass = `badge-cat-${category.replace('backends_', '')}`;

        html += `
            <div class="provider-group-card">
                <div class="provider-group-header">
                    <div class="provider-name">
                        <span class="badge ${catBadgeClass}">${category}</span>
                        <span>${escapeHtml(pId)}</span>
                    </div>
                    <span style="font-size: 11px; color: var(--text-faint); font-weight: 600;">${list.length} backends</span>
                </div>
                <div class="provider-body">
        `;

        list.forEach(be => {
            if (!be) return;
            const status = evaluateBackendStatus(be.meta);

            html += `
                <div class="backend-card">
                    <div class="backend-card-header">
                        <div class="backend-address-group">
                            <span class="status-dot ${status}" title="Statut: ${status}"></span>
                            <span class="backend-address">${escapeHtml(be.address)}</span>
                        </div>
                    </div>
                    <div class="backend-buckets">
                        ${renderMetaBadges(be.meta)}
                    </div>
                </div>
            `;
        });

        html += `</div></div>`;
    });

    if (!html) {
        container.innerHTML = '<div class="empty-placeholder">Aucun backend ne correspond aux critères de recherche</div>';
    } else {
        container.innerHTML = html;
    }
}

export function renderMetricsView() {
    const cardsContainer = document.getElementById('proxies-metrics-cards');
    if (!cardsContainer) return;

    const proxies = state.topology.filter(n => n.category === 'proxy');

    let totalActive = 0;
    let totalProcessed = 0;
    let totalRequests = 0;
    let totalErrors = 0;
    let totalBytesIn = 0;
    let totalBytesOut = 0;

    let html = '';

    proxies.forEach(p => {
        const met = state.proxyMetrics[p.id] || {
            active_connections: 0,
            processed_connections: 0,
            requests: 0,
            errors: 0,
            bytes_in: 0,
            bytes_out: 0
        };
        const rates = state.proxyRates[p.id] || { rate_in: 0, rate_out: 0 };

        totalActive += met.active_connections;
        totalProcessed += met.processed_connections;
        totalRequests += met.requests;
        totalErrors += met.errors;
        totalBytesIn += met.bytes_in;
        totalBytesOut += met.bytes_out;

        html += `
            <div class="proxy-card">
                <div class="proxy-card-header">
                    <div>
                        <span style="font-weight: 700; font-size: 13px;">${escapeHtml(p.name)}</span>
                        <span class="badge badge-cat-proxy" style="margin-left: 6px;">${escapeHtml(p.type)}</span>
                    </div>
                    <span style="font-size: 10px; font-family: var(--font-mono); color: var(--text-faint);">${escapeHtml(p.id)}</span>
                </div>
                <div class="proxy-card-body">
                    <div class="metrics-grid">
                        <div class="metric-card">
                            <span class="metric-label">Cnx Actives</span>
                            <span class="metric-value">${formatNumber(met.active_connections)}</span>
                        </div>
                        <div class="metric-card">
                            <span class="metric-label">Cnx Traitées</span>
                            <span class="metric-value">${formatNumber(met.processed_connections)}</span>
                        </div>
                        <div class="metric-card">
                            <span class="metric-label">Requêtes</span>
                            <span class="metric-value">${formatNumber(met.requests)}</span>
                        </div>
                        <div class="metric-card">
                            <span class="metric-label">Erreurs</span>
                            <span class="metric-value ${met.errors > 0 ? 'has-error' : ''}">${formatNumber(met.errors)}</span>
                        </div>
                        <div class="metric-card">
                            <span class="metric-label">Débit Entrant</span>
                            <span class="metric-value">${formatRate(rates.rate_in)}</span>
                            <span class="metric-subvalue">Total : ${formatBytes(met.bytes_in)}</span>
                        </div>
                        <div class="metric-card">
                            <span class="metric-label">Débit Sortant</span>
                            <span class="metric-value">${formatRate(rates.rate_out)}</span>
                            <span class="metric-subvalue">Total : ${formatBytes(met.bytes_out)}</span>
                        </div>
                    </div>
                    <div style="font-size: 11px; color: var(--text-faint); display: flex; justify-content: space-between;">
                        <span>Source(s) : <strong>${escapeHtml(getNodeSources(p).join(', ') || 'aucune')}</strong></span>
                        <a class="flow-link" onclick="switchTab('topology'); selectNode('${escapeHtml(p.id)}');">Inspecter dans le graphe</a>
                    </div>
                </div>
            </div>
        `;
    });

    // Update top KPIs
    const activeEl = document.getElementById('kpi-active-cnx');
    if (activeEl) activeEl.textContent = formatNumber(totalActive);
    const procEl = document.getElementById('kpi-processed-cnx');
    if (procEl) procEl.textContent = formatNumber(totalProcessed);
    const reqEl = document.getElementById('kpi-requests');
    if (reqEl) reqEl.textContent = formatNumber(totalRequests);

    const errKpi = document.getElementById('kpi-errors');
    if (errKpi) {
        errKpi.textContent = formatNumber(totalErrors);
        if (totalErrors > 0) errKpi.classList.add('danger');
        else errKpi.classList.remove('danger');
    }

    const bIn = document.getElementById('kpi-bytes-in');
    if (bIn) bIn.textContent = formatRate(state.totalRateIn);
    const tIn = document.getElementById('kpi-total-bytes-in');
    if (tIn) tIn.textContent = `Total : ${formatBytes(totalBytesIn)}`;
    const bOut = document.getElementById('kpi-bytes-out');
    if (bOut) bOut.textContent = formatRate(state.totalRateOut);
    const tOut = document.getElementById('kpi-total-bytes-out');
    if (tOut) tOut.textContent = `Total : ${formatBytes(totalBytesOut)}`;

    if (proxies.length === 0) {
        cardsContainer.innerHTML = '<div class="empty-placeholder">Aucun proxy configuré</div>';
    } else {
        cardsContainer.innerHTML = html;
    }
}
