/* ==========================================================================
   MLB Dashboard — Main Application Entrypoint & Orchestration
   ========================================================================== */

import { state } from './state.js';
import { initTheme, cycleTheme } from './theme.js';
import {
    fetchData,
    manualRefresh,
    onAutoRefreshChange,
    setupAutoRefreshTimer,
    updateRefreshText,
    onDataReceived
} from './api.js';
import {
    renderTopology,
    setupPanZoom,
    resetGraphView,
    zoomStep
} from './topology.js';
import {
    selectNode,
    openDrawer,
    closeDrawer,
    renderDrawer,
    setupDrawerResizer,
    filterDrawerBackends
} from './drawer.js';
import {
    renderBackendsView,
    renderMetricsView,
    onGlobalBackendSearch,
    setCategoryFilter
} from './views.js';

export function switchTab(tabId) {
    state.activeTab = tabId;

    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.querySelectorAll('.view-pane').forEach(v => v.classList.remove('active'));

    const btn = document.getElementById(`tab-${tabId}`);
    const pane = document.getElementById(`view-${tabId}`);
    if (btn) btn.classList.add('active');
    if (pane) pane.classList.add('active');

    if (tabId === 'topology') {
        setTimeout(resetGraphView, 50);
    } else if (tabId === 'backends') {
        renderBackendsView();
    } else if (tabId === 'metrics') {
        renderMetricsView();
    }
}

// Subscribe global renderers to data refresh
onDataReceived(() => {
    renderTopology();

    if (!state.hasInitialCentered && state.activeTab === 'topology' && state.topology.length > 0) {
        state.hasInitialCentered = true;
        requestAnimationFrame(() => {
            resetGraphView();
        });
    }

    if (state.activeTab === 'backends') renderBackendsView();
    if (state.activeTab === 'metrics') renderMetricsView();

    // Update drawer if opened
    if (state.selectedNodeId) {
        renderDrawer(state.selectedNodeId);
    }
});

// Bind global handlers to window for HTML inline events (onclick, onchange, oninput)
window.switchTab = switchTab;
window.manualRefresh = manualRefresh;
window.cycleTheme = cycleTheme;
window.zoomStep = zoomStep;
window.resetGraphView = resetGraphView;
window.setCategoryFilter = setCategoryFilter;
window.closeDrawer = closeDrawer;
window.openDrawer = openDrawer;
window.selectNode = selectNode;
window.filterDrawerBackends = filterDrawerBackends;
window.onGlobalBackendSearch = onGlobalBackendSearch;
window.onAutoRefreshChange = onAutoRefreshChange;

/* Keyboard Shortcuts & Observers */
window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
        closeDrawer();
    }
});

window.addEventListener('resize', () => {
    if (state.activeTab === 'topology') {
        resetGraphView();
    }
});

if (window.ResizeObserver) {
    const ro = new ResizeObserver((entries) => {
        for (const entry of entries) {
            if (entry.contentRect && entry.contentRect.width > 0 && entry.contentRect.height > 0) {
                if (state.activeTab === 'topology' && state.topology && state.topology.length > 0) {
                    if (!state.hasInitialCentered) {
                        state.hasInitialCentered = true;
                        resetGraphView();
                    }
                }
            }
        }
    });
    const tc = document.getElementById('topology-container');
    if (tc) ro.observe(tc);
}

// Relative refresh counter loop
setInterval(updateRefreshText, 1000);

// App Boot
window.addEventListener('DOMContentLoaded', () => {
    initTheme();
    setupPanZoom();
    setupDrawerResizer();
    setupAutoRefreshTimer();
    fetchData();
});
