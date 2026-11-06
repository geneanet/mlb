/* ==========================================================================
   MLB Dashboard — SVG Topology Graph Engine & Interactions (Pan, Zoom, Flow)
   ========================================================================== */

import { state, escapeHtml, getNodeSources } from './state.js';
import { computeDAGLayout } from './dag.js';
import { selectNode } from './drawer.js';

export function renderTopology() {
    const edgesLayer = document.getElementById('svg-edges-layer');
    const nodesLayer = document.getElementById('svg-nodes-layer');
    if (!edgesLayer || !nodesLayer) return;

    edgesLayer.innerHTML = '';
    nodesLayer.innerHTML = '';

    if (!state.topology || state.topology.length === 0) {
        nodesLayer.innerHTML = `
            <text x="400" y="200" fill="var(--text-muted)" font-size="14" text-anchor="middle" font-family="sans-serif">
                Aucun module configuré dans MLB
            </text>
        `;
        return;
    }

    const layout = computeDAGLayout(state.topology);

    // Render Edges with Cubic Bezier Curves
    layout.edges.forEach((edge) => {
        const dx = Math.abs(edge.x2 - edge.x1) * 0.5;
        const d = `M ${edge.x1} ${edge.y1} C ${edge.x1 + dx} ${edge.y1}, ${edge.x2 - dx} ${edge.y2}, ${edge.x2} ${edge.y2}`;

        const path = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        path.setAttribute('d', d);
        path.setAttribute('class', 'edge-path');
        path.setAttribute('id', `edge-${edge.from.replace(/\./g, '_')}-${edge.to.replace(/\./g, '_')}`);
        path.setAttribute('data-from', edge.from);
        path.setAttribute('data-to', edge.to);
        path.setAttribute('marker-end', 'url(#arrow)');
        edgesLayer.appendChild(path);
    });

    // Render Nodes
    layout.positions.forEach((pos, id) => {
        const node = pos.node;
        const g = document.createElementNS('http://www.w3.org/2000/svg', 'g');
        g.setAttribute('class', `node-group ${state.selectedNodeId === id ? 'selected' : ''}`);
        g.setAttribute('id', `node-${id.replace(/\./g, '_')}`);
        g.setAttribute('data-id', id);
        g.setAttribute('transform', `translate(${pos.x}, ${pos.y})`);

        const catColors = {
            backends_inventory: 'var(--cat-inventory)',
            backends_processor: 'var(--cat-processor)',
            balancer: 'var(--cat-balancer)',
            proxy: 'var(--cat-proxy)'
        };
        const catHeaderColors = {
            backends_inventory: 'var(--cat-inventory-header, var(--cat-inventory))',
            backends_processor: 'var(--cat-processor-header, var(--cat-processor))',
            balancer: 'var(--cat-balancer-header, var(--cat-balancer))',
            proxy: 'var(--cat-proxy-header, var(--cat-proxy))'
        };
        const catColor = catColors[node.category] || 'var(--accent)';
        const catHeaderColor = catHeaderColors[node.category] || catColor;
        const catShort = node.category.replace('backends_', '').toUpperCase();

        const rawName = node.name || node.id || '';
        const maxNameLen = 17;
        const displayName = rawName.length > maxNameLen ? rawName.slice(0, maxNameLen - 1) + '…' : rawName;

        // Compute info counts (backends or proxy active cnx)
        let countLabel = '';
        if (node.category === 'proxy') {
            const met = state.proxyMetrics[node.id];
            const active = met ? met.active_connections : 0;
            countLabel = `${active} cnx`;
        } else {
            const beList = state.backends[node.id] || [];
            countLabel = `${beList.length} backends`;
        }

        g.innerHTML = `
            <!-- Base Card Rect -->
            <rect class="node-rect" width="${pos.width}" height="${pos.height}" rx="8" ry="8"
                  fill="var(--bg-node)" stroke="var(--border)" stroke-width="1.5" />

            <!-- Top Category Header Stripe -->
            <rect width="${pos.width}" height="22" rx="8" ry="8" fill="${catHeaderColor}" />
            <rect y="12" width="${pos.width}" height="10" fill="${catHeaderColor}" />

            <!-- Category Label -->
            <text x="${pos.width / 2}" y="15" fill="#ffffff" font-size="9" font-weight="700"
                  text-anchor="middle" font-family="var(--font-sans)" letter-spacing="0.05em">
                ${catShort} · ${escapeHtml(node.type.toUpperCase())}
            </text>

            <!-- Module Name -->
            <text x="${pos.width / 2}" y="44" fill="var(--text-main)" font-size="12" font-weight="700"
                  text-anchor="middle" font-family="var(--font-sans)">
                ${escapeHtml(displayName)}
                <title>${escapeHtml(rawName)}</title>
            </text>

            <!-- Stats Subtitle -->
            <text x="${pos.width / 2}" y="65" fill="${catColor}" font-size="11" font-weight="600"
                  text-anchor="middle" font-family="var(--font-sans)">
                ${escapeHtml(countLabel)}
            </text>
        `;

        // Events
        g.addEventListener('click', (e) => {
            e.stopPropagation();
            selectNode(id);
        });

        g.addEventListener('mouseenter', () => highlightNodeFlow(id));
        g.addEventListener('mouseleave', () => resetNodeFlowHighlight());

        nodesLayer.appendChild(g);
    });
}

export function setupPanZoom() {
    const svg = document.getElementById('topology-svg');
    if (!svg) return;

    const activePointers = new Map();
    let lastPinchDistance = null;
    let lastPinchCenter = null;

    svg.addEventListener('pointerdown', (e) => {
        if (e.button !== 0) return;
        if (e.target.closest('.node-group')) return;

        activePointers.set(e.pointerId, { x: e.clientX, y: e.clientY });
        try { svg.setPointerCapture(e.pointerId); } catch (_) {}

        if (activePointers.size === 1) {
            state.isPanning = true;
            state.panStart = { x: e.clientX - state.transform.x, y: e.clientY - state.transform.y };
        } else if (activePointers.size === 2) {
            state.isPanning = false;
            const pts = Array.from(activePointers.values());
            lastPinchDistance = Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y);
            const rect = svg.getBoundingClientRect();
            lastPinchCenter = {
                x: (pts[0].x + pts[1].x) / 2 - rect.left,
                y: (pts[0].y + pts[1].y) / 2 - rect.top
            };
        }
    });

    svg.addEventListener('pointermove', (e) => {
        if (!activePointers.has(e.pointerId)) return;
        activePointers.set(e.pointerId, { x: e.clientX, y: e.clientY });

        if (activePointers.size === 1 && state.isPanning) {
            state.transform.x = e.clientX - state.panStart.x;
            state.transform.y = e.clientY - state.panStart.y;
            applyTransform();
        } else if (activePointers.size === 2 && lastPinchDistance && lastPinchCenter) {
            const pts = Array.from(activePointers.values());
            const currentDistance = Math.hypot(pts[0].x - pts[1].x, pts[0].y - pts[1].y);
            if (currentDistance > 0 && lastPinchDistance > 0) {
                const factor = currentDistance / lastPinchDistance;
                const newScale = Math.min(Math.max(state.transform.scale * factor, 0.15), 3.0);
                const rect = svg.getBoundingClientRect();
                const currentCenter = {
                    x: (pts[0].x + pts[1].x) / 2 - rect.left,
                    y: (pts[0].y + pts[1].y) / 2 - rect.top
                };
                const scaleRatio = newScale / state.transform.scale;
                state.transform.x = currentCenter.x - (currentCenter.x - state.transform.x) * scaleRatio + (currentCenter.x - lastPinchCenter.x);
                state.transform.y = currentCenter.y - (currentCenter.y - state.transform.y) * scaleRatio + (currentCenter.y - lastPinchCenter.y);
                state.transform.scale = newScale;
                lastPinchDistance = currentDistance;
                lastPinchCenter = currentCenter;
                applyTransform();
            }
        }
    });

    const onPointerEnd = (e) => {
        activePointers.delete(e.pointerId);
        try { svg.releasePointerCapture(e.pointerId); } catch (_) {}

        if (activePointers.size === 0) {
            state.isPanning = false;
            lastPinchDistance = null;
            lastPinchCenter = null;
        } else if (activePointers.size === 1) {
            lastPinchDistance = null;
            lastPinchCenter = null;
            const remaining = activePointers.values().next().value;
            state.isPanning = true;
            state.panStart = { x: remaining.x - state.transform.x, y: remaining.y - state.transform.y };
        }
    };

    svg.addEventListener('pointerup', onPointerEnd);
    svg.addEventListener('pointercancel', onPointerEnd);

    svg.addEventListener('wheel', (e) => {
        e.preventDefault();
        const rect = svg.getBoundingClientRect();
        const mouseX = e.clientX - rect.left;
        const mouseY = e.clientY - rect.top;

        const zoomFactor = e.deltaY < 0 ? 1.15 : 0.87;
        const newScale = Math.min(Math.max(state.transform.scale * zoomFactor, 0.25), 3.0);

        state.transform.x = mouseX - (mouseX - state.transform.x) * (newScale / state.transform.scale);
        state.transform.y = mouseY - (mouseY - state.transform.y) * (newScale / state.transform.scale);
        state.transform.scale = newScale;

        applyTransform();
    }, { passive: false });
}

export function applyTransform() {
    const viewport = document.getElementById('svg-viewport');
    if (viewport) {
        viewport.setAttribute('transform', `matrix(${state.transform.scale} 0 0 ${state.transform.scale} ${state.transform.x} ${state.transform.y})`);
    }
}

export function zoomStep(factor) {
    const svg = document.getElementById('topology-container');
    if (!svg) return;
    const cx = svg.clientWidth / 2;
    const cy = svg.clientHeight / 2;

    const newScale = Math.min(Math.max(state.transform.scale * factor, 0.25), 3.0);
    state.transform.x = cx - (cx - state.transform.x) * (newScale / state.transform.scale);
    state.transform.y = cy - (cy - state.transform.y) * (newScale / state.transform.scale);
    state.transform.scale = newScale;
    applyTransform();
}

export function resetGraphView() {
    if (state.activeTab !== 'topology') return;
    const container = document.getElementById('topology-container');
    if (!container || !state.topology || state.topology.length === 0) return;

    const cw = container.clientWidth;
    const ch = container.clientHeight;
    if (cw <= 0 || ch <= 0) {
        requestAnimationFrame(resetGraphView);
        return;
    }

    const layout = computeDAGLayout(state.topology);
    const padding = cw < 500 ? 16 : 60;
    const scaleX = (cw - padding * 2) / Math.max(layout.width, 1);
    const scaleY = (ch - padding * 2) / Math.max(layout.height, 1);
    const scale = Math.min(Math.min(scaleX, scaleY), 1.05);

    state.transform.scale = Math.max(scale, 0.15);
    state.transform.x = (cw - layout.width * state.transform.scale) / 2;
    state.transform.y = (ch - layout.height * state.transform.scale) / 2;
    applyTransform();
}

export function highlightNodeFlow(nodeId) {
    const nodeMap = new Map();
    state.topology.forEach(n => nodeMap.set(n.id, n));

    const directNodeIds = new Set();
    const indirectNodeIds = new Set();
    const directEdgeKeys = new Set();
    const indirectEdgeKeys = new Set();

    const targetNode = nodeMap.get(nodeId);

    // Direct parents
    const targetSources = getNodeSources(targetNode);
    targetSources.forEach(src => {
        if (nodeMap.has(src)) {
            directNodeIds.add(src);
            directEdgeKeys.add(`${src}->${nodeId}`);
        }
    });

    // Direct children
    state.topology.filter(n => getNodeSources(n).includes(nodeId)).forEach(child => {
        directNodeIds.add(child.id);
        directEdgeKeys.add(`${nodeId}->${child.id}`);
    });

    // 1. Traverse all upstream ancestors back to root inventory
    const queueUp = [...targetSources];
    const visitedUp = new Set([nodeId, ...targetSources]);
    targetSources.forEach(src => indirectNodeIds.add(src));

    while (queueUp.length > 0) {
        const currId = queueUp.shift();
        const parentNode = nodeMap.get(currId);
        if (parentNode) {
            const parentSources = getNodeSources(parentNode);
            parentSources.forEach(pSrc => {
                indirectEdgeKeys.add(`${pSrc}->${currId}`);
                if (!visitedUp.has(pSrc)) {
                    visitedUp.add(pSrc);
                    indirectNodeIds.add(pSrc);
                    queueUp.push(pSrc);
                }
            });
        }
    }

    // 2. Traverse all downstream descendants to proxies
    const queueDown = Array.from(directNodeIds).filter(id => !targetSources.includes(id));
    const visitedDown = new Set([nodeId, ...queueDown]);

    while (queueDown.length > 0) {
        const curr = queueDown.shift();
        const children = state.topology.filter(n => getNodeSources(n).includes(curr));
        children.forEach(child => {
            indirectEdgeKeys.add(`${curr}->${child.id}`);
            if (!visitedDown.has(child.id)) {
                visitedDown.add(child.id);
                indirectNodeIds.add(child.id);
                queueDown.push(child.id);
            }
        });
    }

    // Update connecting edges
    const edges = document.querySelectorAll('.edge-path');
    edges.forEach(edge => {
        const from = edge.getAttribute('data-from');
        const to = edge.getAttribute('data-to');
        const key = `${from}->${to}`;

        edge.classList.remove('highlighted-direct', 'highlighted-indirect', 'dimmed');

        if (directEdgeKeys.has(key)) {
            edge.classList.add('highlighted-direct');
            edge.setAttribute('marker-end', 'url(#arrow-highlight)');
        } else if (indirectEdgeKeys.has(key)) {
            edge.classList.add('highlighted-indirect');
            edge.setAttribute('marker-end', 'url(#arrow-highlight)');
        } else {
            edge.classList.add('dimmed');
            edge.setAttribute('marker-end', 'url(#arrow)');
        }
    });

    // Update nodes styling
    const nodes = document.querySelectorAll('.node-group');
    nodes.forEach(nodeEl => {
        const id = nodeEl.getAttribute('data-id');
        nodeEl.classList.remove('flow-direct', 'flow-indirect');

        if (directNodeIds.has(id)) {
            nodeEl.classList.add('flow-direct');
        } else if (indirectNodeIds.has(id)) {
            nodeEl.classList.add('flow-indirect');
        }
    });
}

export function resetNodeFlowHighlight() {
    document.querySelectorAll('.edge-path').forEach(edge => {
        edge.classList.remove('highlighted-direct', 'highlighted-indirect', 'dimmed');
        edge.setAttribute('marker-end', 'url(#arrow)');
    });
    document.querySelectorAll('.node-group').forEach(nodeEl => {
        nodeEl.classList.remove('flow-direct', 'flow-indirect');
    });
}
