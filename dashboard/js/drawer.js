/* ==========================================================================
   MLB Dashboard — Module Drawer (Inspection Sidebar) & HCL Syntax Highlighting
   ========================================================================== */

import {
    state,
    formatBytes,
    formatRate,
    formatNumber,
    escapeHtml,
    getNodeSources,
    resolveModuleBackends,
    evaluateBackendStatus,
    renderMetaBadges
} from './state.js';

export function selectNode(nodeId) {
    state.selectedNodeId = nodeId;
    document.querySelectorAll('.node-group').forEach(n => {
        if (n.getAttribute('data-id') === nodeId) n.classList.add('selected');
        else n.classList.remove('selected');
    });
    openDrawer(nodeId);
}

export function openDrawer(nodeId) {
    state.drawerFilterQuery = '';
    const filterInput = document.getElementById('drawer-backend-filter');
    if (filterInput) filterInput.value = '';
    renderDrawer(nodeId);
    const drawer = document.getElementById('module-drawer');
    if (drawer) drawer.classList.add('open');
}

export function closeDrawer() {
    state.selectedNodeId = null;
    state.drawerFilterQuery = '';
    const filterInput = document.getElementById('drawer-backend-filter');
    if (filterInput) filterInput.value = '';
    document.querySelectorAll('.node-group').forEach(n => n.classList.remove('selected'));
    const drawer = document.getElementById('module-drawer');
    if (drawer) drawer.classList.remove('open');
}

export function renderDrawer(nodeId) {
    const targetId = nodeId || state.selectedNodeId;
    if (!targetId) return;

    const node = state.topology.find(n => n.id === targetId);
    if (!node) return;

    const titleEl = document.getElementById('drawer-title');
    if (titleEl) titleEl.textContent = node.name;

    // Badges
    const badgesEl = document.getElementById('drawer-badges');
    if (badgesEl) {
        const catBadgeClass = `badge-cat-${node.category.replace('backends_', '')}`;
        badgesEl.innerHTML = `
            <span class="badge ${catBadgeClass}">${escapeHtml(node.category)}</span>
            <span class="badge" style="background: var(--bg-surface); border: 1px solid var(--border); color: var(--text-muted);">${escapeHtml(node.type)}</span>
            <span style="font-family: var(--font-mono); font-size: 10px; color: var(--text-faint);">${escapeHtml(node.id)}</span>
        `;
    }

    // Routing
    const routingEl = document.getElementById('drawer-routing');
    if (routingEl) {
        let routingHtml = '';
        const nodeSources = getNodeSources(node);
        if (nodeSources.length === 1) {
            routingHtml += `<span>Amont : <a class="flow-link" data-select-node="${escapeHtml(nodeSources[0])}">${escapeHtml(nodeSources[0])}</a></span>`;
        } else if (nodeSources.length > 1) {
            const links = nodeSources.map(s => `<a class="flow-link" data-select-node="${escapeHtml(s)}">${escapeHtml(s)}</a>`).join(', ');
            routingHtml += `<span>Amont (${nodeSources.length}) : ${links}</span>`;
        } else {
            routingHtml += `<span style="color: var(--text-faint);">Racine (aucun amont)</span>`;
        }

        // Downstream consumers
        const consumers = state.topology.filter(n => getNodeSources(n).includes(node.id));
        if (consumers.length > 0) {
            const links = consumers.map(c => `<a class="flow-link" data-select-node="${escapeHtml(c.id)}">${escapeHtml(c.id)}</a>`).join(', ');
            routingHtml += ` <span style="color: var(--text-faint); margin: 0 4px;">➔</span> <span>Aval : ${links}</span>`;
        }
        routingEl.innerHTML = routingHtml;
        routingEl.querySelectorAll('[data-select-node]').forEach(el => {
            el.addEventListener('click', () => selectNode(el.dataset.selectNode));
        });
    }

    // Proxy Metrics
    const metricsSec = document.getElementById('drawer-proxy-metrics-sec');
    if (metricsSec) {
        if (node.category === 'proxy') {
            metricsSec.style.display = 'flex';
            const met = state.proxyMetrics[node.id] || {
                active_connections: 0,
                processed_connections: 0,
                requests: 0,
                errors: 0,
                bytes_in: 0,
                bytes_out: 0
            };
            const actEl = document.getElementById('drawer-metric-active-cnx');
            if (actEl) actEl.textContent = formatNumber(met.active_connections);
            const procEl = document.getElementById('drawer-metric-processed-cnx');
            if (procEl) procEl.textContent = formatNumber(met.processed_connections);
            const reqEl = document.getElementById('drawer-metric-requests');
            if (reqEl) reqEl.textContent = formatNumber(met.requests);

            const errEl = document.getElementById('drawer-metric-errors');
            if (errEl) {
                errEl.textContent = formatNumber(met.errors);
                if (met.errors > 0) errEl.classList.add('has-error');
                else errEl.classList.remove('has-error');
            }

            const rates = state.proxyRates[node.id] || { rate_in: 0, rate_out: 0 };
            const bIn = document.getElementById('drawer-metric-bytes-in');
            if (bIn) bIn.textContent = formatRate(rates.rate_in);
            const tIn = document.getElementById('drawer-metric-total-in');
            if (tIn) tIn.textContent = `Total : ${formatBytes(met.bytes_in)}`;
            const bOut = document.getElementById('drawer-metric-bytes-out');
            if (bOut) bOut.textContent = formatRate(rates.rate_out);
            const tOut = document.getElementById('drawer-metric-total-out');
            if (tOut) tOut.textContent = `Total : ${formatBytes(met.bytes_out)}`;
        } else {
            metricsSec.style.display = 'none';
        }
    }

    // Backends
    const resolved = resolveModuleBackends(node.id);
    const countEl = document.getElementById('drawer-backends-count');
    if (countEl) countEl.textContent = resolved.backends.length;

    const hintEl = document.getElementById('drawer-backends-source-hint');
    if (hintEl) {
        if (resolved.resolvedFrom) {
            hintEl.textContent = `(depuis ${resolved.resolvedFrom})`;
        } else {
            hintEl.textContent = '';
        }
    }

    renderDrawerBackendCards(resolved.backends);

    // Raw HCL with Syntax Highlighting
    const hclEl = document.getElementById('drawer-hcl');
    if (hclEl) {
        if (node.hcl) {
            hclEl.innerHTML = highlightHCL(node.hcl);
        } else {
            hclEl.innerHTML = '<span class="hcl-comment"># Aucune configuration HCL disponible</span>';
        }
    }
}

export function filterDrawerBackends(query) {
    state.drawerFilterQuery = query.toLowerCase().trim();
    if (!state.selectedNodeId) return;
    const resolved = resolveModuleBackends(state.selectedNodeId);
    renderDrawerBackendCards(resolved.backends);
}

export function renderDrawerBackendCards(backends) {
    const listEl = document.getElementById('drawer-backends-list');
    if (!listEl) return;

    if (!backends || backends.length === 0) {
        listEl.innerHTML = '<div class="empty-placeholder">Aucun backend découvert</div>';
        return;
    }

    let sorted = [...backends]
        .filter(b => b != null)
        .sort((a, b) => (a.address || '').localeCompare(b.address || ''));
    let filtered = sorted;
    if (state.drawerFilterQuery) {
        filtered = sorted.filter(b => {
            const addr = (b.address || '').toLowerCase();
            if (addr.includes(state.drawerFilterQuery)) return true;
            if (b.meta && JSON.stringify(b.meta).toLowerCase().includes(state.drawerFilterQuery)) return true;
            return false;
        });
    }

    if (filtered.length === 0) {
        listEl.innerHTML = '<div class="empty-placeholder">Aucun résultat correspondant au filtre</div>';
        return;
    }

    let html = '';
    filtered.forEach(be => {
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

    listEl.innerHTML = html;
}

export function setupDrawerResizer() {
    const resizer = document.getElementById('drawer-resizer');
    const drawer = document.getElementById('module-drawer');
    if (!resizer || !drawer) return;

    resizer.addEventListener('mousedown', (e) => {
        e.preventDefault();
        state.isResizingDrawer = true;
        resizer.classList.add('resizing');

        const onMouseMove = (ev) => {
            if (!state.isResizingDrawer) return;
            const newWidth = window.innerWidth - ev.clientX;
            if (newWidth >= 320 && newWidth <= window.innerWidth * 0.75) {
                state.drawerWidth = newWidth;
                drawer.style.width = `${newWidth}px`;
            }
        };

        const onMouseUp = () => {
            state.isResizingDrawer = false;
            resizer.classList.remove('resizing');
            window.removeEventListener('mousemove', onMouseMove);
            window.removeEventListener('mouseup', onMouseUp);
        };

        window.addEventListener('mousemove', onMouseMove);
        window.addEventListener('mouseup', onMouseUp);
    });
}

// HCL Syntax Highlighter (Zero External Dependency)
export function highlightHCL(code) {
    if (!code) return '';

    const tokenRegex = /(#(?:[^\n]*)|(?:\/\/)[^\n]*|\/\*[\s\S]*?\*\/)|("(?:[^"\\]|\\.)*")|(\b(?:true|false|null)\b)|(\b(?:backends_inventory|backends_processor|balancer|proxy|metrics|system|rlimit)\b)|(\b(?:contains|int|parseint|lower|upper|length)\b(?=\s*\())|(\b\d+(?:\.\d+)?(?:ms|[smhd]|kb|mb|gb)?\b)|(\b[a-zA-Z_][a-zA-Z0-9_-]*(?:\.[a-zA-Z_][a-zA-Z0-9_-]*)*\b)|([=!<>]=?|&&|\|\||[{}()[\],=])/g;

    let html = '';
    let lastIndex = 0;
    let match;

    while ((match = tokenRegex.exec(code)) !== null) {
        if (match.index > lastIndex) {
            html += escapeHtml(code.slice(lastIndex, match.index));
        }

        const [full, comment, str, boolVal, blockType, func, num, ident, op] = match;

        if (comment) {
            html += `<span class="hcl-comment">${escapeHtml(comment)}</span>`;
        } else if (str) {
            html += `<span class="hcl-string">${escapeHtml(str)}</span>`;
        } else if (boolVal) {
            html += `<span class="hcl-bool">${escapeHtml(boolVal)}</span>`;
        } else if (blockType) {
            html += `<span class="hcl-block">${escapeHtml(blockType)}</span>`;
        } else if (func) {
            html += `<span class="hcl-func">${escapeHtml(func)}</span>`;
        } else if (num) {
            html += `<span class="hcl-number">${escapeHtml(num)}</span>`;
        } else if (ident) {
            if (ident.startsWith('backend.meta') || ident.includes('.')) {
                html += `<span class="hcl-ref">${escapeHtml(ident)}</span>`;
            } else {
                html += `<span class="hcl-ident">${escapeHtml(ident)}</span>`;
            }
        } else if (op) {
            html += `<span class="hcl-op">${escapeHtml(op)}</span>`;
        }

        lastIndex = tokenRegex.lastIndex;
    }

    if (lastIndex < code.length) {
        html += escapeHtml(code.slice(lastIndex));
    }

    return html;
}
