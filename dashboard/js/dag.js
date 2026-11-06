/* ==========================================================================
   MLB Dashboard — SVG Topology DAG Engine with Crossing Reduction (Sugiyama)
   ========================================================================== */

import { getNodeSources } from './state.js';

export function computeDAGLayout(nodes) {
    if (!nodes || nodes.length === 0) return { positions: new Map(), edges: [], width: 800, height: 400 };

    const nodeMap = new Map();
    nodes.forEach(n => nodeMap.set(n.id, n));

    // 1. Build adjacency list for connected components (undirected)
    const adj = new Map();
    nodes.forEach(n => adj.set(n.id, []));
    nodes.forEach(n => {
        const srcs = getNodeSources(n);
        srcs.forEach(src => {
            if (nodeMap.has(src)) {
                adj.get(n.id).push(src);
                adj.get(src).push(n.id);
            }
        });
    });

    // 2. Discover independent connected components (disjoint service pipelines)
    const visited = new Set();
    const components = [];

    nodes.forEach(n => {
        if (!visited.has(n.id)) {
            const comp = [];
            const queue = [n.id];
            visited.add(n.id);

            while (queue.length > 0) {
                const curr = queue.shift();
                comp.push(nodeMap.get(curr));

                (adj.get(curr) || []).forEach(neighbor => {
                    if (!visited.has(neighbor)) {
                        visited.add(neighbor);
                        queue.push(neighbor);
                    }
                });
            }
            components.push(comp);
        }
    });

    // Sort components: smaller/compact pipelines first, then multi-branch clusters
    components.sort((a, b) => a.length - b.length);

    const colSpacing = 260;
    const nodeWidth = 170;
    const nodeHeight = 84;
    const rowGap = 32;
    const compGap = 56; // Separation between independent service bands to prevent cross-pipeline intersections

    const positions = new Map();
    let currentY = 50;
    let maxColIndex = 0;

    // 3. Layout each component separately in its own horizontal band
    components.forEach((compNodes) => {
        const compNodeMap = new Map();
        compNodes.forEach(n => compNodeMap.set(n.id, n));

        // Compute topological depth / rank within this component
        const ranks = new Map();
        const recursionStack = new Set();

        function getRank(nodeId) {
            if (recursionStack.has(nodeId)) return 0;
            if (ranks.has(nodeId)) return ranks.get(nodeId);

            const node = compNodeMap.get(nodeId);
            if (!node) return 0;

            recursionStack.add(nodeId);
            let rank = 0;
            const srcs = getNodeSources(node);
            srcs.forEach(src => {
                if (compNodeMap.has(src)) {
                    const r = getRank(src) + 1;
                    if (r > rank) rank = r;
                }
            });
            recursionStack.delete(nodeId);
            ranks.set(nodeId, rank);
            return rank;
        }

        compNodes.forEach(n => getRank(n.id));

        // Group component nodes into columns
        const columns = new Map();
        compNodes.forEach(n => {
            const r = ranks.get(n.id);
            if (!columns.has(r)) columns.set(r, []);
            columns.get(r).push(n);
        });

        const sortedRanks = Array.from(columns.keys()).sort((a, b) => a - b);
        sortedRanks.forEach(r => { if (r > maxColIndex) maxColIndex = r; });

        // Within-component crossing reduction using Sugiyama barycenter heuristic
        if (sortedRanks.length > 1) {
            const upstream = new Map();
            const downstream = new Map();
            compNodes.forEach(n => {
                upstream.set(n.id, []);
                downstream.set(n.id, []);
            });
            compNodes.forEach(n => {
                const srcs = getNodeSources(n);
                srcs.forEach(src => {
                    if (compNodeMap.has(src)) {
                        upstream.get(n.id).push(src);
                        downstream.get(src).push(n.id);
                    }
                });
            });

            // Forward and backward sweeps to align branches horizontally
            for (let pass = 0; pass < 6; pass++) {
                // Forward sweep: align nodes with upstream sources
                for (let i = 1; i < sortedRanks.length; i++) {
                    const r = sortedRanks[i];
                    const colNodes = columns.get(r);
                    if (!colNodes || colNodes.length <= 1) continue;

                    const prevIndices = new Map();
                    columns.get(sortedRanks[i - 1]).forEach((n, idx) => prevIndices.set(n.id, idx));

                    const bcs = new Map();
                    colNodes.forEach((node, originalIdx) => {
                        const ups = upstream.get(node.id) || [];
                        const validUps = ups.filter(uId => prevIndices.has(uId));
                        if (validUps.length > 0) {
                            const avg = validUps.reduce((acc, uId) => acc + prevIndices.get(uId), 0) / validUps.length;
                            bcs.set(node.id, avg);
                        } else {
                            bcs.set(node.id, originalIdx);
                        }
                    });
                    colNodes.sort((a, b) => bcs.get(a.id) - bcs.get(b.id));
                }

                // Backward sweep: align nodes with downstream consumers
                for (let i = sortedRanks.length - 2; i >= 0; i--) {
                    const r = sortedRanks[i];
                    const colNodes = columns.get(r);
                    if (!colNodes || colNodes.length <= 1) continue;

                    const nextIndices = new Map();
                    columns.get(sortedRanks[i + 1]).forEach((n, idx) => nextIndices.set(n.id, idx));

                    const bcs = new Map();
                    colNodes.forEach((node, originalIdx) => {
                        const downs = downstream.get(node.id) || [];
                        const validDowns = downs.filter(dId => nextIndices.has(dId));
                        if (validDowns.length > 0) {
                            const avg = validDowns.reduce((acc, dId) => acc + nextIndices.get(dId), 0) / validDowns.length;
                            bcs.set(node.id, avg);
                        } else {
                            bcs.set(node.id, originalIdx);
                        }
                    });
                    colNodes.sort((a, b) => bcs.get(a.id) - bcs.get(b.id));
                }
            }
        }

        // Compute bounding box for this component band
        let compMaxRows = 1;
        sortedRanks.forEach(r => {
            const count = columns.get(r).length;
            if (count > compMaxRows) compMaxRows = count;
        });

        const compHeight = compMaxRows * nodeHeight + (compMaxRows - 1) * rowGap;

        // Assign positions
        sortedRanks.forEach(r => {
            const colNodes = columns.get(r);
            const count = colNodes.length;
            const colHeight = count * nodeHeight + (count - 1) * rowGap;
            const startY = currentY + (compHeight - colHeight) / 2;
            const x = 50 + r * colSpacing;

            colNodes.forEach((node, rowIdx) => {
                const y = startY + rowIdx * (nodeHeight + rowGap);
                positions.set(node.id, {
                    x,
                    y,
                    width: nodeWidth,
                    height: nodeHeight,
                    rank: r,
                    node
                });
            });
        });

        currentY += compHeight + compGap;
    });

    const totalWidth = (maxColIndex + 1) * colSpacing + 100;
    const totalHeight = Math.max(currentY, 420);

    // Generate edges
    const edges = [];
    nodes.forEach(n => {
        const srcs = getNodeSources(n);
        srcs.forEach(src => {
            if (positions.has(src) && positions.has(n.id)) {
                const fromPos = positions.get(src);
                const toPos = positions.get(n.id);

                edges.push({
                    from: src,
                    to: n.id,
                    x1: fromPos.x + fromPos.width,
                    y1: fromPos.y + fromPos.height / 2,
                    x2: toPos.x,
                    y2: toPos.y + toPos.height / 2
                });
            }
        });
    });

    return { positions, edges, width: totalWidth, height: totalHeight };
}
