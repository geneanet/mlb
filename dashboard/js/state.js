/* ==========================================================================
   MLB Dashboard — State Management & Utility Functions
   ========================================================================== */

export const state = {
    topology: [],
    backends: {},
    proxyMetrics: {},
    selectedNodeId: null,
    activeTab: 'topology',
    autoRefreshSec: 5,
    autoRefreshTimer: null,
    lastRefreshTime: null,
    isFetching: false,
    fetchError: false,
    globalSearchQuery: '',
    categoryFilter: 'all',
    drawerFilterQuery: '',
    theme: 'auto',
    hasInitialCentered: false,

    // SVG Pan & Zoom transform
    transform: { x: 0, y: 0, scale: 1 },
    isPanning: false,
    panStart: { x: 0, y: 0 },

    // Drawer resizer state
    drawerWidth: 600,
    isResizingDrawer: false,

    // Metrics rate calculation
    prevProxyMetrics: null,
    prevMetricsTime: null,
    proxyRates: {},
    totalRateIn: 0,
    totalRateOut: 0
};

export function formatBytes(bytes) {
    if (!bytes || isNaN(bytes) || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const absBytes = Math.abs(bytes);
    const i = Math.min(Math.floor(Math.log(absBytes) / Math.log(k)), sizes.length - 1);
    const sign = bytes < 0 ? '-' : '';
    return sign + parseFloat((absBytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

export function formatRate(bytesPerSec) {
    if (!bytesPerSec || isNaN(bytesPerSec) || bytesPerSec <= 0) return '0 B/s';
    return formatBytes(bytesPerSec) + '/s';
}

export function formatNumber(num) {
    if (!num || isNaN(num)) return '0';
    return new Intl.NumberFormat().format(num);
}

// Heuristic evaluation of backend status dot
export function evaluateBackendStatus(meta) {
    if (!meta || typeof meta !== 'object') return 'neutral';

    let hasErr = false;
    let hasOk = false;

    for (const bucketName in meta) {
        const bucket = meta[bucketName];
        if (!bucket || typeof bucket !== 'object') continue;

        for (const key in bucket) {
            const val = bucket[key];

            // Explicit string statuses (MySQL, Redis, etc.)
            if (key === 'status') {
                if (val === 'err' || val === 'down' || val === 'error' || val === 'failed') hasErr = true;
                if (val === 'ok' || val === 'up' || val === 'alive') hasOk = true;
            }
            // Boolean health flags (Consul passing, active, healthy)
            if (key === 'active' || key === 'passing' || key === 'healthy') {
                if (val === false || val === 0 || val === 'false') hasErr = true;
                if (val === true || val === 1 || val === 'true') hasOk = true;
            }
            if (key === 'master_link_status') {
                if (val === 'down') hasErr = true;
                if (val === 'up') hasOk = true;
            }
        }
    }

    if (hasErr) return 'err';
    if (hasOk) return 'ok';
    return 'neutral';
}

// Helper to get all upstream sources for a module (supporting source or sources array)
export function getNodeSources(node) {
    if (!node) return [];
    if (Array.isArray(node.sources) && node.sources.length > 0) return node.sources;
    if (node.source) return [node.source];
    return [];
}

// Resolve backends for a module (including upstream resolution for proxies)
export function resolveModuleBackends(moduleId) {
    if (!moduleId) return { backends: [], resolvedFrom: null };

    // Direct backends check
    if (state.backends[moduleId] && state.backends[moduleId].length > 0) {
        return { backends: state.backends[moduleId], resolvedFrom: null };
    }

    // Upstream resolution if proxy or module without direct entries
    const node = state.topology.find(n => n.id === moduleId);
    if (node) {
        const nodeSources = getNodeSources(node);
        const visited = new Set([moduleId]);
        const queue = [...nodeSources];

        while (queue.length > 0) {
            const currId = queue.shift();
            if (!currId || visited.has(currId)) continue;
            visited.add(currId);

            if (state.backends[currId] && state.backends[currId].length > 0) {
                return { backends: state.backends[currId], resolvedFrom: currId };
            }
            const currNode = state.topology.find(n => n.id === currId);
            if (currNode) {
                queue.push(...getNodeSources(currNode));
            }
        }
    }

    return { backends: [], resolvedFrom: null };
}

export function escapeHtml(str) {
    if (str === null || str === undefined) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

// Render bucket metadata badges HTML
export function renderMetaBadges(meta) {
    if (!meta || typeof meta !== 'object' || Object.keys(meta).length === 0) {
        return '<div style="color: var(--text-faint); font-size: 11px; font-style: italic;">Aucune métadonnée</div>';
    }

    let html = '';
    for (const bucketName in meta) {
        const bucket = meta[bucketName];
        if (!bucket || typeof bucket !== 'object') continue;

        let bucketClass = 'default';
        if (bucketName.includes('mysql')) bucketClass = 'mysql';
        else if (bucketName.includes('redis')) bucketClass = 'redis';
        else if (bucketName.includes('consul')) bucketClass = 'consul';

        const entries = Object.entries(bucket);
        if (entries.length === 0) continue;

        html += `<div class="bucket-row">`;
        html += `<span class="bucket-name-tag">${escapeHtml(bucketName)}</span>`;

        for (const [key, val] of entries) {
            let displayVal = val;
            if (Array.isArray(val)) {
                displayVal = val.join(', ');
            } else if (typeof val === 'object' && val !== null) {
                displayVal = JSON.stringify(val);
            }
            html += `<span class="badge-bucket ${bucketClass}" title="${escapeHtml(bucketName + '.' + key + ' = ' + displayVal)}">${escapeHtml(key)}: ${escapeHtml(String(displayVal))}</span>`;
        }
        html += `</div>`;
    }

    return html;
}
