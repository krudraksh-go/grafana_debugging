// KPI Architecture Tree Visualization
// Professional D3.js Interactive Tree

(function() {
    'use strict';

    // =============================================
    // Configuration
    // =============================================
    const CONFIG = {
        nodeRadius: {
            dashboard: 10,
            kpi: 8,
            metric: 7,
            derived: 7,
            root: 7,
            calculation: 6,
            external: 6,
            column: 5,
            root_column: 5,
            tag: 4,
            external_table: 5,
            external_column: 5,
            note: 4
        },
        spacing: {
            horizontal: 280,
            vertical: 45
        },
        animation: {
            duration: 500
        },
        initialScale: 0.7,
        minZoom: 0.1,
        maxZoom: 3
    };

    // =============================================
    // State
    // =============================================
    let svg, g, zoom, tree, root;
    let currentView = 'tree';
    let selectedNode = null;
    let visibleTypes = new Set(['kpi', 'metric', 'derived', 'root', 'column', 'external', 'calculation', 'root_column', 'tag', 'external_table', 'external_column', 'note', 'dashboard']);
    let searchTerm = '';
    let nodeIdCounter = 0;
    let activeCriticalPath = null;
    let healthFilterStatus = null;

    // =============================================
    // Initialization
    // =============================================
    function init() {
        // Assign unique IDs to all nodes
        assignIds(KPI_ARCHITECTURE_DATA);
        
        // Setup SVG
        setupSVG();
        
        // Setup tree layout
        setupTree();
        
        // Create initial visualization
        update(root);
        
        // Setup event listeners
        setupEventListeners();
        
        // Setup minimap
        setupMinimap();
        
        // Setup health monitoring
        setupHealthMonitoring();
        
        // Hide loading overlay
        setTimeout(() => {
            document.getElementById('loading').classList.add('hidden');
        }, 500);
        
        // Center the view
        setTimeout(centerView, 600);
    }
    
    // =============================================
    // Health Monitoring Setup
    // =============================================
    function setupHealthMonitoring() {
        // Update health counts
        updateHealthCounts();
        
        // Populate critical paths list
        populateCriticalPaths();
        
        // Setup health filter click handlers
        document.querySelectorAll('.health-filter').forEach(item => {
            item.addEventListener('click', () => {
                const status = item.dataset.health;
                if (healthFilterStatus === status) {
                    healthFilterStatus = null;
                    item.classList.remove('active');
                } else {
                    document.querySelectorAll('.health-filter').forEach(i => i.classList.remove('active'));
                    healthFilterStatus = status;
                    item.classList.add('active');
                    expandToHealthStatus(status);
                }
                update(root);
            });
        });
    }
    
    function updateHealthCounts() {
        const summary = getHealthSummary();
        
        // Update summary cards
        const criticalEl = document.getElementById('summary-critical');
        const warningEl = document.getElementById('summary-warning');
        const healthyEl = document.getElementById('summary-healthy');
        
        if (criticalEl) criticalEl.textContent = summary.critical;
        if (warningEl) warningEl.textContent = summary.warning;
        if (healthyEl) healthyEl.textContent = summary.healthy;
        
        // Populate critical KPIs list
        populateCriticalKPIs();
    }
    
    function populateCriticalKPIs() {
        const container = document.getElementById('critical-kpis-list');
        if (!container) return;
        
        // Get all KPIs with health data, sorted by severity
        const kpisWithHealth = [];
        for (const [name, health] of Object.entries(MOCK_HEALTH_DATA)) {
            const threshold = HEALTH_THRESHOLDS[name];
            if (threshold) {
                kpisWithHealth.push({ name, health, threshold });
            }
        }
        
        // Sort: critical first, then warning, then by value deviation from target
        kpisWithHealth.sort((a, b) => {
            const severityOrder = { critical: 0, warning: 1, healthy: 2 };
            const aSeverity = severityOrder[a.health.status] || 3;
            const bSeverity = severityOrder[b.health.status] || 3;
            return aSeverity - bSeverity;
        });
        
        // Show top 6 most critical
        const topKPIs = kpisWithHealth.slice(0, 6);
        
        let html = '';
        topKPIs.forEach(kpi => {
            const trendIcon = kpi.health.trend === 'up' ? '↑' : (kpi.health.trend === 'down' ? '↓' : '→');
            const trendClass = kpi.health.trend;
            const deviation = kpi.threshold.target ? 
                Math.round(((kpi.health.value - kpi.threshold.target) / kpi.threshold.target) * 100) : 0;
            const deviationText = deviation > 0 ? `+${deviation}%` : `${deviation}%`;
            
            html += `
                <div class="kpi-metric-card ${kpi.health.status}" data-kpi-name="${kpi.name}">
                    <div class="kpi-metric-info">
                        <div class="kpi-metric-name">${kpi.name}</div>
                        <div class="kpi-metric-target">Target: ${kpi.threshold.target} ${kpi.threshold.unit}</div>
                    </div>
                    <div class="kpi-metric-value-container">
                        <div class="kpi-metric-value ${kpi.health.status}">${kpi.health.value}</div>
                        <div class="kpi-metric-unit">${kpi.threshold.unit}</div>
                        <div class="kpi-metric-trend ${trendClass}">${trendIcon} ${deviationText}</div>
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
        
        // Add click handlers
        container.querySelectorAll('.kpi-metric-card').forEach(card => {
            card.addEventListener('click', () => {
                const name = card.dataset.kpiName;
                const node = findNodeByName(name);
                if (node) {
                    // Expand path to this node
                    let current = node;
                    while (current) {
                        if (current._children) {
                            current.children = current._children;
                            current._children = null;
                        }
                        current = current.parent;
                    }
                    update(root);
                    showDetails(node);
                    setTimeout(() => panToNode(node), 300);
                }
            });
        });
    }
    
    function populateCriticalPaths() {
        const container = document.getElementById('critical-paths-list');
        if (!container) return;
        
        let html = '';
        CRITICAL_PATHS.forEach(path => {
            const severityClass = path.severity === 'warning' ? 'warning' : '';
            html += `
                <div class="critical-path-card ${severityClass}" data-path-id="${path.id}">
                    <div class="critical-path-title">
                        <span>${path.severity === 'critical' ? '🔴' : '🟡'}</span>
                        ${path.title}
                    </div>
                    <div class="critical-path-desc">${path.description}</div>
                    <div class="critical-path-chain">
                        ${path.path.slice(0, 4).map((node, i) => `
                            <span class="chain-node ${severityClass}">${truncateLabel(node, 12)}</span>
                            ${i < 3 ? '<span class="chain-arrow">→</span>' : ''}
                        `).join('')}
                        ${path.path.length > 4 ? '<span class="chain-arrow">→ ...</span>' : ''}
                    </div>
                    <div class="critical-path-impact ${severityClass}">
                        <span class="impact-label ${severityClass}">Impact:</span> ${path.impact}
                    </div>
                </div>
            `;
        });
        
        container.innerHTML = html;
        
        // Add click handlers for critical path cards
        container.querySelectorAll('.critical-path-card').forEach(card => {
            card.addEventListener('click', () => {
                const pathId = parseInt(card.dataset.pathId);
                selectCriticalPath(pathId);
            });
        });
    }
    
    function selectCriticalPath(pathId) {
        const path = CRITICAL_PATHS.find(p => p.id === pathId);
        if (!path) return;
        
        // Toggle active state
        if (activeCriticalPath === pathId) {
            activeCriticalPath = null;
            document.querySelectorAll('.critical-path-card').forEach(c => c.classList.remove('active'));
        } else {
            activeCriticalPath = pathId;
            document.querySelectorAll('.critical-path-card').forEach(c => {
                c.classList.toggle('active', parseInt(c.dataset.pathId) === pathId);
            });
            
            // Expand nodes in the path
            expandCriticalPath(path.path);
            
            // Show path details in the details panel
            showCriticalPathDetails(path);
        }
        
        update(root);
    }
    
    function expandCriticalPath(pathNames) {
        // First expand all nodes in the path
        root.descendants().forEach(d => {
            if (pathNames.includes(d.data.name)) {
                // Expand this node and all ancestors
                let current = d;
                while (current) {
                    if (current._children) {
                        current.children = current._children;
                        current._children = null;
                    }
                    current = current.parent;
                }
            }
        });
        
        // Pan to the first node in path
        const firstNode = findNodeByName(pathNames[0]);
        if (firstNode) {
            setTimeout(() => panToNode(firstNode), 300);
        }
    }
    
    function expandToHealthStatus(status) {
        root.descendants().forEach(d => {
            const health = getNodeHealth(d.data.name);
            if (health && health.status === status) {
                let current = d;
                while (current) {
                    if (current._children) {
                        current.children = current._children;
                        current._children = null;
                    }
                    current = current.parent;
                }
            }
        });
    }
    
    function showCriticalPathDetails(path) {
        const panel = document.getElementById('details-panel');
        
        const severityColor = path.severity === 'critical' ? '#dc2626' : '#f59e0b';
        const severityBg = path.severity === 'critical' ? '#fef2f2' : '#fffbeb';
        
        let pathHTML = path.path.map((node, i) => {
            const health = getNodeHealth(node);
            const statusClass = health ? health.status : '';
            return `
                <div style="display: flex; align-items: center; gap: 8px; padding: 6px 0; ${i > 0 ? 'border-top: 1px dashed #e5e7eb;' : ''}">
                    <span style="color: ${severityColor};">${i === path.path.length - 1 ? '🎯' : '↓'}</span>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 12px;">${node}</span>
                    ${health ? `<span class="health-badge ${statusClass}">${health.value}</span>` : ''}
                </div>
            `;
        }).join('');
        
        let detailsHTML = `
            <div class="details-header" style="background: ${severityBg};">
                <div class="details-title">
                    <span style="font-size: 18px;">${path.severity === 'critical' ? '🔴' : '🟡'}</span>
                    ${path.title}
                </div>
            </div>
            <div class="details-content">
                <div class="detail-row">
                    <span class="detail-label">Severity</span>
                    <span class="health-badge ${path.severity}">${path.severity.toUpperCase()}</span>
                </div>
                <div class="detail-row" style="flex-direction: column;">
                    <span class="detail-label" style="margin-bottom: 8px;">Issue Chain (${path.path.length} nodes)</span>
                    <div style="background: ${severityBg}; border-radius: 8px; padding: 12px; border: 1px solid ${severityColor}20;">
                        ${pathHTML}
                    </div>
                </div>
                <div class="detail-row" style="flex-direction: column; margin-top: 12px;">
                    <span class="detail-label">Root Cause</span>
                    <span class="detail-value" style="color: ${severityColor}; font-weight: 600;">${path.rootCause}</span>
                </div>
                <div class="detail-row" style="flex-direction: column; margin-top: 8px;">
                    <span class="detail-label">Impact</span>
                    <span class="detail-value">${path.impact}</span>
                </div>
                <div class="detail-row" style="flex-direction: column; margin-top: 8px;">
                    <span class="detail-label">Recommendation</span>
                    <span class="detail-value" style="color: #059669;">${path.recommendation}</span>
                </div>
            </div>
        `;
        
        panel.innerHTML = detailsHTML;
    }
    
    function isNodeInActivePath(nodeName) {
        if (!activeCriticalPath) return false;
        const path = CRITICAL_PATHS.find(p => p.id === activeCriticalPath);
        return path && path.path.includes(nodeName);
    }
    
    function getActivePathSeverity() {
        if (!activeCriticalPath) return null;
        const path = CRITICAL_PATHS.find(p => p.id === activeCriticalPath);
        return path ? path.severity : null;
    }

    function assignIds(node, depth = 0) {
        node.id = ++nodeIdCounter;
        node.depth = depth;
        if (node.children) {
            node.children.forEach(child => assignIds(child, depth + 1));
        }
    }

    // =============================================
    // SVG Setup
    // =============================================
    function setupSVG() {
        const container = document.querySelector('.tree-container');
        const width = container.clientWidth;
        const height = container.clientHeight;

        svg = d3.select('#tree-svg')
            .attr('width', width)
            .attr('height', height);

        // Clear any existing content
        svg.selectAll('*').remove();

        // Add defs for gradients and filters
        const defs = svg.append('defs');

        // Glow filter for nodes
        const filter = defs.append('filter')
            .attr('id', 'glow')
            .attr('x', '-50%')
            .attr('y', '-50%')
            .attr('width', '200%')
            .attr('height', '200%');

        filter.append('feGaussianBlur')
            .attr('stdDeviation', '3')
            .attr('result', 'coloredBlur');

        const feMerge = filter.append('feMerge');
        feMerge.append('feMergeNode').attr('in', 'coloredBlur');
        feMerge.append('feMergeNode').attr('in', 'SourceGraphic');

        // Create gradient for links
        const gradient = defs.append('linearGradient')
            .attr('id', 'link-gradient')
            .attr('gradientUnits', 'userSpaceOnUse');

        gradient.append('stop')
            .attr('offset', '0%')
            .attr('stop-color', '#3b82f6')
            .attr('stop-opacity', 0.6);

        gradient.append('stop')
            .attr('offset', '100%')
            .attr('stop-color', '#8b5cf6')
            .attr('stop-opacity', 0.6);

        // Setup zoom behavior
        zoom = d3.zoom()
            .scaleExtent([CONFIG.minZoom, CONFIG.maxZoom])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
                updateMinimap();
            });

        svg.call(zoom);

        // Create main group
        g = svg.append('g')
            .attr('class', 'tree-group');

        // Links group (behind nodes)
        g.append('g').attr('class', 'links-group');

        // Nodes group
        g.append('g').attr('class', 'nodes-group');
    }

    // =============================================
    // Tree Layout
    // =============================================
    function setupTree() {
        tree = d3.tree()
            .nodeSize([CONFIG.spacing.vertical, CONFIG.spacing.horizontal])
            .separation((a, b) => {
                return a.parent === b.parent ? 1 : 1.5;
            });

        root = d3.hierarchy(KPI_ARCHITECTURE_DATA);
        
        // Collapse all but first two levels initially
        root.descendants().forEach((d, i) => {
            if (d.depth > 1) {
                d._children = d.children;
                d.children = null;
            }
        });
    }

    // =============================================
    // Update Visualization
    // =============================================
    function update(source) {
        const treeData = tree(root);
        const nodes = treeData.descendants();
        const links = treeData.links();

        // Normalize for fixed-depth
        nodes.forEach(d => {
            d.y = d.depth * CONFIG.spacing.horizontal;
        });

        // =============================================
        // NODES
        // =============================================
        const nodeGroup = g.select('.nodes-group');
        
        const node = nodeGroup.selectAll('.node')
            .data(nodes, d => d.data.id);

        // Enter new nodes
        const nodeEnter = node.enter()
            .append('g')
            .attr('class', d => {
                let classes = `node node-${d.data.type}`;
                if (isNodeInActivePath(d.data.name)) {
                    const severity = getActivePathSeverity();
                    classes += severity === 'critical' ? ' critical-node' : ' warning-node';
                }
                const health = getNodeHealth(d.data.name);
                if (health && health.status === 'critical') {
                    classes += ' health-pulse';
                }
                return classes;
            })
            .attr('transform', `translate(${source.y0 || 0},${source.x0 || 0})`)
            .style('opacity', 0);

        // Small status indicator circle
        nodeEnter.append('circle')
            .attr('r', d => getNodeRadius(d.data.type))
            .attr('fill', d => getNodeColor(d.data.type, d.data))
            .attr('stroke', d => getNodeColor(d.data.type, d.data))
            .attr('stroke-opacity', 0.3)
            .attr('cursor', 'pointer')
            .on('click', (event, d) => {
                event.stopPropagation();
                toggleNode(d);
            })
            .on('mouseover', showTooltip)
            .on('mousemove', moveTooltip)
            .on('mouseout', hideTooltip);

        // Expand/collapse indicator (only if has children)
        nodeEnter.filter(d => d.children || d._children)
            .append('text')
            .attr('class', 'expand-indicator')
            .attr('dy', '0.35em')
            .attr('text-anchor', 'middle')
            .attr('font-size', 8)
            .attr('fill', '#fff')
            .attr('pointer-events', 'none')
            .text(d => d._children ? '+' : '−');

        // VALUE - Large and prominent (for nodes with health data)
        nodeEnter.filter(d => getNodeHealth(d.data.name))
            .append('text')
            .attr('class', 'label-value')
            .attr('dy', '-0.3em')
            .attr('x', d => getNodeRadius(d.data.type) + 12)
            .attr('text-anchor', 'start')
            .attr('fill', d => {
                const health = getNodeHealth(d.data.name);
                return health ? HEALTH_COLORS[health.status] : '#2d3748';
            })
            .attr('font-size', 20)
            .attr('font-weight', 700)
            .attr('font-family', "'JetBrains Mono', monospace")
            .text(d => {
                const health = getNodeHealth(d.data.name);
                const threshold = getNodeThreshold(d.data.name);
                if (health && threshold) {
                    return `${health.value} ${threshold.unit}`;
                }
                return health ? health.value : '';
            });

        // KPI NAME - Clear and readable
        nodeEnter.append('text')
            .attr('class', 'label-primary')
            .attr('dy', d => getNodeHealth(d.data.name) ? '1.1em' : '0.35em')
            .attr('x', d => getNodeRadius(d.data.type) + 12)
            .attr('text-anchor', 'start')
            .attr('fill', '#2d3748')
            .attr('font-size', 13)
            .attr('font-weight', 600)
            .text(d => truncateLabel(d.data.name, 30));

        // Secondary info - deviation or DB info
        nodeEnter.append('text')
            .attr('class', 'label-secondary')
            .attr('dy', d => getNodeHealth(d.data.name) ? '2.3em' : '1.6em')
            .attr('x', d => getNodeRadius(d.data.type) + 12)
            .attr('text-anchor', 'start')
            .attr('fill', d => {
                const health = getNodeHealth(d.data.name);
                if (health) {
                    return health.status === 'critical' ? '#b85450' : 
                           (health.status === 'warning' ? '#c4824a' : '#48a999');
                }
                return '#718096';
            })
            .attr('font-size', 10)
            .text(d => getSecondaryLabel(d.data));

        // Add verification badge
        nodeEnter.filter(d => d.data.verified === true)
            .append('circle')
            .attr('class', 'verified-badge')
            .attr('r', 5)
            .attr('cx', d => -getNodeRadius(d.data.type) - 3)
            .attr('cy', d => -getNodeRadius(d.data.type) / 2)
            .attr('fill', '#10b981');

        nodeEnter.filter(d => d.data.verified === true)
            .append('text')
            .attr('class', 'verified-check')
            .attr('x', d => -getNodeRadius(d.data.type) - 3)
            .attr('y', d => -getNodeRadius(d.data.type) / 2 + 1)
            .attr('text-anchor', 'middle')
            .attr('dominant-baseline', 'middle')
            .attr('font-size', 7)
            .attr('fill', '#fff')
            .text('✓');

        // Update
        const nodeUpdate = nodeEnter.merge(node);

        nodeUpdate
            .attr('class', d => {
                let classes = `node node-${d.data.type}`;
                if (isNodeInActivePath(d.data.name)) {
                    const severity = getActivePathSeverity();
                    classes += severity === 'critical' ? ' critical-node' : ' warning-node';
                }
                const health = getNodeHealth(d.data.name);
                if (health && health.status === 'critical') {
                    classes += ' health-pulse';
                }
                return classes;
            })
            .transition()
            .duration(CONFIG.animation.duration)
            .attr('transform', d => `translate(${d.y},${d.x})`)
            .style('opacity', d => isNodeVisible(d) ? 1 : 0.2);

        nodeUpdate.select('circle')
            .attr('r', d => getNodeRadius(d.data.type))
            .attr('fill', d => isNodeHighlighted(d) ? d3.color(getNodeColor(d.data.type, d.data)).brighter(0.3) : getNodeColor(d.data.type, d.data))
            .attr('stroke-width', d => isNodeInActivePath(d.data.name) ? 2.5 : 1.5);

        nodeUpdate.select('.expand-indicator')
            .text(d => d._children ? '+' : (d.children ? '−' : ''));
        
        // Update value colors for highlighted nodes
        nodeUpdate.select('.label-value')
            .attr('fill', d => {
                const health = getNodeHealth(d.data.name);
                return health ? HEALTH_COLORS[health.status] : '#2d3748';
            });

        // Exit
        node.exit()
            .transition()
            .duration(CONFIG.animation.duration)
            .attr('transform', `translate(${source.y},${source.x})`)
            .style('opacity', 0)
            .remove();

        // =============================================
        // LINKS
        // =============================================
        const linkGroup = g.select('.links-group');

        const link = linkGroup.selectAll('.link')
            .data(links, d => d.target.data.id);

        // Enter new links
        const linkEnter = link.enter()
            .insert('path', 'g')
            .attr('class', 'link')
            .attr('d', d => {
                const o = { x: source.x0 || 0, y: source.y0 || 0 };
                return diagonal(o, o);
            })
            .style('opacity', 0);

        // Update
        const linkUpdate = linkEnter.merge(link);

        linkUpdate
            .attr('class', d => {
                let classes = 'link';
                if (isNodeInActivePath(d.source.data.name) && isNodeInActivePath(d.target.data.name)) {
                    const severity = getActivePathSeverity();
                    classes += severity === 'critical' ? ' critical-path' : ' warning-path';
                }
                return classes;
            })
            .transition()
            .duration(CONFIG.animation.duration)
            .attr('d', d => diagonal(d.source, d.target))
            .attr('stroke', d => {
                if (isNodeInActivePath(d.source.data.name) && isNodeInActivePath(d.target.data.name)) {
                    return getActivePathSeverity() === 'critical' ? '#b85450' : '#c4824a';
                }
                return isLinkHighlighted(d) ? '#4a6fa5' : '#c4c9cf';
            })
            .attr('stroke-width', d => {
                if (isNodeInActivePath(d.source.data.name) && isNodeInActivePath(d.target.data.name)) {
                    return 3;
                }
                return isLinkHighlighted(d) ? 2 : 1.5;
            })
            .style('opacity', d => isNodeVisible(d.source) && isNodeVisible(d.target) ? 0.6 : 0.1);

        // Exit
        link.exit()
            .transition()
            .duration(CONFIG.animation.duration)
            .attr('d', d => {
                const o = { x: source.x, y: source.y };
                return diagonal(o, o);
            })
            .style('opacity', 0)
            .remove();

        // Store positions for animation
        nodes.forEach(d => {
            d.x0 = d.x;
            d.y0 = d.y;
        });

        // Update minimap
        updateMinimap();
    }

    // Diagonal path generator
    function diagonal(s, d) {
        return `M ${s.y} ${s.x}
                C ${(s.y + d.y) / 2} ${s.x},
                  ${(s.y + d.y) / 2} ${d.x},
                  ${d.y} ${d.x}`;
    }

    // =============================================
    // Node Helpers
    // =============================================
    function getNodeRadius(type) {
        return CONFIG.nodeRadius[type] || 10;
    }

    function getNodeColor(type, data) {
        // Check health status first
        if (data && data.name) {
            const health = getNodeHealth(data.name);
            if (health) {
                if (health.status === 'critical') return HEALTH_COLORS.critical;
                if (health.status === 'warning') return HEALTH_COLORS.warning;
                if (health.status === 'healthy') return HEALTH_COLORS.healthy;
            }
        }
        
        // If dbNotFound, use orange color
        if (data && data.dbNotFound) {
            return '#d97706';
        }
        // If has database, use green
        if (data && data.database) {
            return '#059669';
        }
        return NODE_TYPES[type]?.color || '#64748b';
    }

    function getTypeLabel(data) {
        // Check health status first and show value
        if (data && data.name) {
            const health = getNodeHealth(data.name);
            const threshold = getNodeThreshold(data.name);
            if (health && threshold) {
                const trendIcon = health.trend === 'up' ? '↑' : (health.trend === 'down' ? '↓' : '→');
                const statusIcon = health.status === 'critical' ? '🔴' : (health.status === 'warning' ? '🟡' : '🟢');
                return `${statusIcon} ${health.value} ${threshold.unit} ${trendIcon}`;
            } else if (health) {
                const statusIcon = health.status === 'critical' ? '🔴' : (health.status === 'warning' ? '🟡' : '🟢');
                return `${statusIcon} ${health.status.toUpperCase()}`;
            }
        }
        
        // Show DB NOT FOUND badge
        if (data.dbNotFound) return '⚠️ DB NOT FOUND';
        // Show database name if available
        if (data.database) return data.database;
        if (data.schedule) return data.schedule;
        if (data.dataType) return data.dataType;
        if (data.source) return data.source;
        if (data.formula) return '📐 Formula';
        return NODE_TYPES[data.type]?.label || '';
    }

    function truncateLabel(text, maxLength) {
        if (!text) return '';
        return text.length > maxLength ? text.substring(0, maxLength) + '...' : text;
    }
    
    function getSecondaryLabel(data) {
        const health = getNodeHealth(data.name);
        if (health) {
            const threshold = getNodeThreshold(data.name);
            if (threshold && threshold.target) {
                const deviation = Math.round(((health.value - threshold.target) / threshold.target) * 100);
                const trendIcon = health.trend === 'up' ? '↑' : (health.trend === 'down' ? '↓' : '→');
                const sign = deviation > 0 ? '+' : '';
                return `${trendIcon} ${sign}${deviation}% from target`;
            }
            return health.status.toUpperCase();
        }
        
        if (data.dbNotFound) return '⚠ DB NOT FOUND';
        if (data.database) return data.database;
        if (data.formula) return '📐 Formula';
        return NODE_TYPES[data.type]?.label || '';
    }

    function isNodeVisible(d) {
        if (searchTerm && !matchesSearch(d)) return false;
        return visibleTypes.has(d.data.type);
    }

    function isNodeHighlighted(d) {
        if (!searchTerm) return selectedNode === d;
        return matchesSearch(d);
    }

    function isLinkHighlighted(link) {
        return isNodeHighlighted(link.target) || isNodeHighlighted(link.source);
    }

    function matchesSearch(d) {
        if (!searchTerm) return true;
        const term = searchTerm.toLowerCase();
        const name = (d.data.name || '').toLowerCase();
        const description = (d.data.description || '').toLowerCase();
        const formula = (d.data.formula || '').toLowerCase();
        return name.includes(term) || description.includes(term) || formula.includes(term);
    }

    // =============================================
    // Node Interactions
    // =============================================
    function toggleNode(d) {
        if (d.children) {
            d._children = d.children;
            d.children = null;
        } else if (d._children) {
            d.children = d._children;
            d._children = null;
        }
        update(d);
        showDetails(d);
    }

    function showDetails(d) {
        selectedNode = d;
        const panel = document.getElementById('details-panel');
        
        const typeConfig = NODE_TYPES[d.data.type] || { icon: '📄', label: 'Node', color: '#64748b' };
        const health = getNodeHealth(d.data.name);
        const threshold = getNodeThreshold(d.data.name);
        
        // Determine header color based on health
        let headerBg = '#f0fdf4';
        let headerIcon = typeConfig.icon;
        if (health) {
            if (health.status === 'critical') {
                headerBg = '#fef2f2';
                headerIcon = '🔴';
            } else if (health.status === 'warning') {
                headerBg = '#fffbeb';
                headerIcon = '🟡';
            } else {
                headerBg = '#f0fdf4';
                headerIcon = '🟢';
            }
        } else if (d.data.dbNotFound) {
            headerBg = '#fef3c7';
        }
        
        // DB status badge
        const dbStatus = d.data.dbNotFound ? 
            '<span style="background: #d97706; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;">⚠️ DB NOT FOUND</span>' : 
            (d.data.database ? '<span style="background: #059669; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;">✓ DB MAPPED</span>' : '');

        let detailsHTML = `
            <div class="details-header" style="background: ${headerBg};">
                <div class="details-title">
                    <span style="font-size: 18px;">${headerIcon}</span>
                    ${d.data.name}
                </div>
            </div>
            <div class="details-content">
        `;
        
        // Health status section (if available)
        if (health) {
            const trendIcon = health.trend === 'up' ? '↑' : (health.trend === 'down' ? '↓' : '→');
            const trendClass = health.trend === 'up' ? 'trend-up' : (health.trend === 'down' ? 'trend-down' : 'trend-stable');
            const unit = threshold ? threshold.unit : '';
            const target = threshold ? threshold.target : null;
            const deviation = target ? Math.round(((health.value - target) / target) * 100) : 0;
            const deviationSign = deviation > 0 ? '+' : '';
            const statusBg = health.status === 'critical' ? '#2d1f1f' : (health.status === 'warning' ? '#2d261f' : '#1f2d29');
            
            detailsHTML += `
                <div style="background: ${statusBg}; border-radius: 8px; padding: 16px; margin-bottom: 12px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                        <span style="font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #a0aec0;">Current Value</span>
                        <span class="health-badge ${health.status}">${health.status.toUpperCase()}</span>
                    </div>
                    <div style="text-align: center;">
                        <div style="font-size: 42px; font-weight: 800; font-family: 'JetBrains Mono', monospace; color: ${HEALTH_COLORS[health.status]}; line-height: 1;">
                            ${health.value}
                        </div>
                        <div style="font-size: 12px; color: #718096; margin-top: 4px;">${unit}</div>
                    </div>
                    ${target !== null ? `
                        <div style="margin-top: 16px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.1);">
                            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                                <span style="font-size: 11px; color: #718096;">Target: ${target} ${unit}</span>
                                <span style="font-size: 13px; font-weight: 700; color: ${HEALTH_COLORS[health.status]};">${deviationSign}${deviation}%</span>
                            </div>
                            <div style="height: 8px; background: rgba(255,255,255,0.1); border-radius: 4px; overflow: hidden;">
                                <div style="height: 100%; width: ${Math.min(100, Math.max(0, (health.value / target) * 100))}%; background: ${HEALTH_COLORS[health.status]}; border-radius: 4px; transition: width 0.5s;"></div>
                            </div>
                            <div style="display: flex; justify-content: space-between; margin-top: 4px; font-size: 9px; color: #4a5568;">
                                <span>0</span>
                                <span>${target}</span>
                                <span>${Math.round(target * 1.5)}</span>
                            </div>
                        </div>
                    ` : ''}
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 12px; padding-top: 8px; border-top: 1px solid rgba(255,255,255,0.05);">
                        <span style="font-size: 10px; color: #4a5568;">Trend: <span class="${trendClass}" style="font-weight: 600;">${trendIcon} ${health.trend}</span></span>
                        <span style="font-size: 9px; color: #4a5568;">Updated ${health.lastUpdated}</span>
                    </div>
                </div>
            `;
            
            // Show related critical paths
            const relatedPaths = getCriticalPathsForNode(d.data.name);
            if (relatedPaths.length > 0) {
                detailsHTML += `
                    <div style="margin-top: 12px; padding: 12px; background: #fef2f2; border-radius: 8px; border: 1px solid #fecaca;">
                        <div style="font-size: 11px; text-transform: uppercase; color: #991b1b; font-weight: 600; margin-bottom: 8px;">
                            ⚠️ Part of ${relatedPaths.length} Issue Chain(s)
                        </div>
                        ${relatedPaths.map(p => `
                            <div style="font-size: 12px; color: #991b1b; cursor: pointer; padding: 4px 0;" onclick="document.querySelector('[data-path-id=\\'${p.id}\\']').click()">
                                → ${p.title}
                            </div>
                        `).join('')}
                    </div>
                `;
            }
        }
        
        detailsHTML += `
                <div class="detail-row" style="margin-top: 12px;">
                    <span class="detail-label">Type</span>
                    <span class="detail-value type-badge" style="background: ${typeConfig.color}; color: white;">
                        ${typeConfig.label}
                    </span>
                </div>
        `;

        // DB Status
        if (dbStatus) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">DB Status</span>
                    <span class="detail-value">${dbStatus}</span>
                </div>
            `;
        }

        if (d.data.description) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Description</span>
                    <span class="detail-value">${d.data.description}</span>
                </div>
            `;
        }

        // Database info
        if (d.data.database) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Database</span>
                    <span class="detail-value" style="color: #059669; font-weight: 600;">${d.data.database}</span>
                </div>
            `;
        }

        // Columns
        if (d.data.columns && d.data.columns.length > 0) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Columns</span>
                    <span class="detail-value" style="font-family: 'JetBrains Mono', monospace; font-size: 11px;">
                        ${d.data.columns.join(', ')}
                    </span>
                </div>
            `;
        }

        // Query
        if (d.data.query) {
            detailsHTML += `
                <div class="detail-row" style="flex-direction: column;">
                    <span class="detail-label" style="margin-bottom: 4px;">Query (NOT EXECUTED)</span>
                    <span class="detail-value" style="font-family: 'JetBrains Mono', monospace; background: #1e293b; color: #e2e8f0; padding: 12px; border-radius: 6px; display: block; font-size: 10px; white-space: pre-wrap; word-break: break-all; max-height: 120px; overflow-y: auto;">
${d.data.query}
                    </span>
                </div>
            `;
        }

        // Formula
        if (d.data.formula) {
            detailsHTML += `
                <div class="detail-row" style="flex-direction: column;">
                    <span class="detail-label" style="margin-bottom: 4px;">Formula</span>
                    <span class="detail-value" style="font-family: 'JetBrains Mono', monospace; background: #ede9fe; color: #5b21b6; padding: 12px; border-radius: 6px; display: block; font-size: 11px;">
                        ${d.data.formula}
                    </span>
                </div>
            `;
        }

        // Source file
        if (d.data.sourceFile) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Source File</span>
                    <span class="detail-value" style="font-family: 'JetBrains Mono', monospace; font-size: 11px;">${d.data.sourceFile}</span>
                </div>
            `;
        }

        // Note (for DB NOT FOUND)
        if (d.data.note) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Note</span>
                    <span class="detail-value" style="color: #d97706; font-style: italic;">${d.data.note}</span>
                </div>
            `;
        }

        // Children count
        const childCount = (d.children?.length || 0) + (d._children?.length || 0);
        if (childCount > 0) {
            detailsHTML += `
                <div class="detail-row">
                    <span class="detail-label">Children</span>
                    <span class="detail-value">${childCount} nodes</span>
                </div>
            `;
        }

        detailsHTML += `</div>`;
        panel.innerHTML = detailsHTML;

        update(d);
    }

    // =============================================
    // Tooltip
    // =============================================
    function showTooltip(event, d) {
        const tooltip = document.getElementById('tooltip');
        const typeConfig = NODE_TYPES[d.data.type] || { icon: '📄', label: 'Node', color: '#64748b' };
        const health = getNodeHealth(d.data.name);
        const threshold = getNodeThreshold(d.data.name);
        
        // Determine status color
        let statusColor = typeConfig.color;
        let statusLabel = typeConfig.label;
        
        if (health) {
            statusColor = HEALTH_COLORS[health.status] || statusColor;
            const trendIcon = health.trend === 'up' ? '↑' : (health.trend === 'down' ? '↓' : '→');
            statusLabel = `${health.status.toUpperCase()} ${trendIcon}`;
        } else if (d.data.dbNotFound) {
            statusColor = '#d97706';
            statusLabel = 'DB NOT FOUND';
        } else if (d.data.database) {
            statusColor = '#059669';
            statusLabel = 'DB MAPPED';
        }

        let content = `
            <div class="tooltip-header">
                <div class="tooltip-dot" style="background: ${statusColor};"></div>
                <span class="tooltip-title">${d.data.name}</span>
                <span class="tooltip-type" style="background: ${statusColor}; color: white;">
                    ${statusLabel}
                </span>
            </div>
        `;

        // Show health value prominently if available
        if (health && threshold) {
            const trendIcon = health.trend === 'up' ? '↑' : (health.trend === 'down' ? '↓' : '→');
            const trendColor = health.trend === 'up' ? '#dc2626' : (health.trend === 'down' ? '#10b981' : '#6b7280');
            content += `
                <div style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">
                    <div style="display: flex; align-items: baseline; gap: 8px;">
                        <span style="font-size: 24px; font-weight: 700; color: ${statusColor};">${health.value}</span>
                        <span style="color: #6b7280; font-size: 12px;">${threshold.unit}</span>
                        <span style="color: ${trendColor}; margin-left: auto;">${trendIcon}</span>
                    </div>
                    <div style="font-size: 11px; color: #6b7280; margin-top: 4px;">
                        Target: ${threshold.target} ${threshold.unit}
                    </div>
                </div>
            `;
        } else if (health) {
            content += `
                <div style="padding: 8px 0; border-bottom: 1px solid #e5e7eb;">
                    <span class="health-badge ${health.status}">${health.status.toUpperCase()}</span>
                </div>
            `;
        }

        // Show database or formula info
        if (d.data.database) {
            content += `<div class="tooltip-content" style="color: #059669;"><strong>📦 ${d.data.database}</strong></div>`;
        } else if (d.data.formula) {
            content += `<div class="tooltip-content" style="color: #7c3aed;"><strong>📐 ${d.data.formula}</strong></div>`;
        } else if (d.data.note) {
            content += `<div class="tooltip-content" style="color: #d97706;"><em>⚠️ ${d.data.note}</em></div>`;
        }

        const tags = [];
        if (d.data.columns && d.data.columns.length > 0) {
            tags.push(`Columns: ${d.data.columns.length}`);
        }
        if (d.data.sourceFile) tags.push(d.data.sourceFile);
        if (d.data.query) tags.push('Has Query');
        if (health) tags.push(`Updated: ${health.lastUpdated}`);

        if (tags.length > 0) {
            content += `<div class="tooltip-meta">`;
            tags.forEach(tag => {
                content += `<span class="tooltip-tag">${tag}</span>`;
            });
            content += `</div>`;
        }

        tooltip.innerHTML = content;
        tooltip.classList.add('visible');
        moveTooltip(event);
    }

    function moveTooltip(event) {
        const tooltip = document.getElementById('tooltip');
        const rect = document.querySelector('.tree-container').getBoundingClientRect();
        let x = event.clientX - rect.left + 15;
        let y = event.clientY - rect.top + 15;

        // Keep tooltip in viewport
        const tooltipRect = tooltip.getBoundingClientRect();
        if (x + tooltipRect.width > rect.width) {
            x = event.clientX - rect.left - tooltipRect.width - 15;
        }
        if (y + tooltipRect.height > rect.height) {
            y = event.clientY - rect.top - tooltipRect.height - 15;
        }

        tooltip.style.left = x + 'px';
        tooltip.style.top = y + 'px';
    }

    function hideTooltip() {
        document.getElementById('tooltip').classList.remove('visible');
    }

    // =============================================
    // Minimap
    // =============================================
    function setupMinimap() {
        const minimapSvg = d3.select('#minimap-svg')
            .attr('width', '100%')
            .attr('height', '100%');

        minimapSvg.append('g').attr('class', 'minimap-tree');
        minimapSvg.append('rect').attr('class', 'minimap-viewport');
    }

    function updateMinimap() {
        const minimapSvg = d3.select('#minimap-svg');
        const minimapTree = minimapSvg.select('.minimap-tree');
        const viewport = minimapSvg.select('.minimap-viewport');

        const treeData = tree(root);
        const nodes = treeData.descendants();
        
        if (nodes.length === 0) return;

        // Calculate bounds
        const xExtent = d3.extent(nodes, d => d.x);
        const yExtent = d3.extent(nodes, d => d.y);

        const width = 200;
        const height = 120;
        const padding = 10;

        const treeWidth = yExtent[1] - yExtent[0] || 1;
        const treeHeight = xExtent[1] - xExtent[0] || 1;

        const scale = Math.min(
            (width - 2 * padding) / treeWidth,
            (height - 2 * padding) / treeHeight
        );

        // Draw minimap tree
        minimapTree.selectAll('circle').remove();
        
        nodes.forEach(d => {
            minimapTree.append('circle')
                .attr('cx', padding + (d.y - yExtent[0]) * scale)
                .attr('cy', padding + (d.x - xExtent[0]) * scale)
                .attr('r', 2)
                .attr('fill', getNodeColor(d.data.type, d.data))
                .attr('opacity', 0.8);
        });

        // Draw viewport rectangle
        const transform = d3.zoomTransform(svg.node());
        const container = document.querySelector('.tree-container');
        const containerWidth = container.clientWidth;
        const containerHeight = container.clientHeight;

        const vpX = (-transform.x / transform.k - yExtent[0]) * scale + padding;
        const vpY = (-transform.y / transform.k - xExtent[0]) * scale + padding;
        const vpW = (containerWidth / transform.k) * scale;
        const vpH = (containerHeight / transform.k) * scale;

        viewport
            .attr('x', Math.max(0, vpX))
            .attr('y', Math.max(0, vpY))
            .attr('width', Math.min(width, vpW))
            .attr('height', Math.min(height, vpH));
    }

    // =============================================
    // Event Listeners
    // =============================================
    function setupEventListeners() {
        // Search
        const searchInput = document.getElementById('search-input');
        searchInput.addEventListener('input', (e) => {
            searchTerm = e.target.value;
            if (searchTerm) {
                // Expand all nodes that match search
                expandToMatches();
            }
            update(root);
        });

        // View toggle
        document.querySelectorAll('.view-toggle-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.view-toggle-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentView = btn.dataset.view;
                if (currentView === 'radial') {
                    setupRadialLayout();
                } else {
                    setupTree();
                }
                update(root);
            });
        });

        // Expand/Collapse all
        document.getElementById('expand-all').addEventListener('click', expandAll);
        document.getElementById('collapse-all').addEventListener('click', collapseAll);
        document.getElementById('reset-view').addEventListener('click', () => {
            collapseAll();
            setTimeout(centerView, 100);
        });

        // Zoom controls
        document.getElementById('zoom-in').addEventListener('click', () => {
            svg.transition().duration(300).call(zoom.scaleBy, 1.3);
        });
        document.getElementById('zoom-out').addEventListener('click', () => {
            svg.transition().duration(300).call(zoom.scaleBy, 0.7);
        });
        document.getElementById('zoom-fit').addEventListener('click', centerView);

        // Legend filtering
        document.querySelectorAll('.legend-item').forEach(item => {
            item.addEventListener('click', () => {
                const type = item.dataset.type;
                item.classList.toggle('active');
                if (item.classList.contains('active')) {
                    visibleTypes.add(type);
                    // Add related types
                    if (type === 'root') visibleTypes.add('root_column');
                    if (type === 'column') visibleTypes.add('tag');
                    if (type === 'external') {
                        visibleTypes.add('external_table');
                        visibleTypes.add('external_column');
                    }
                } else {
                    visibleTypes.delete(type);
                    if (type === 'root') visibleTypes.delete('root_column');
                    if (type === 'column') visibleTypes.delete('tag');
                    if (type === 'external') {
                        visibleTypes.delete('external_table');
                        visibleTypes.delete('external_column');
                    }
                }
                update(root);
            });
        });

        // Quick links
        document.querySelectorAll('.quick-link').forEach(link => {
            link.addEventListener('click', () => {
                const target = link.dataset.target;
                searchInput.value = target;
                searchTerm = target;
                expandToMatches();
                update(root);
                
                // Find and highlight the node
                const node = findNodeByName(target);
                if (node) {
                    showDetails(node);
                    panToNode(node);
                }
            });
        });

        // Window resize
        window.addEventListener('resize', () => {
            const container = document.querySelector('.tree-container');
            svg.attr('width', container.clientWidth)
               .attr('height', container.clientHeight);
            updateMinimap();
        });
    }

    // =============================================
    // Navigation Helpers
    // =============================================
    function expandAll() {
        // Recursive function to expand all nodes including hidden ones
        function expandNode(node) {
            if (node._children) {
                node.children = node._children;
                node._children = null;
            }
            if (node.children) {
                node.children.forEach(child => expandNode(child));
            }
        }
        expandNode(root);
        update(root);
    }

    function collapseAll() {
        root.descendants().forEach(d => {
            if (d.depth > 1 && d.children) {
                d._children = d.children;
                d.children = null;
            }
        });
        update(root);
    }

    function expandToMatches() {
        root.descendants().forEach(d => {
            if (matchesSearch(d)) {
                // Expand all ancestors
                let current = d.parent;
                while (current) {
                    if (current._children) {
                        current.children = current._children;
                        current._children = null;
                    }
                    current = current.parent;
                }
            }
        });
    }

    function findNodeByName(name) {
        let found = null;
        root.descendants().forEach(d => {
            if (d.data.name.toLowerCase().includes(name.toLowerCase())) {
                found = d;
            }
        });
        return found;
    }

    function panToNode(node) {
        const container = document.querySelector('.tree-container');
        const x = node.y;
        const y = node.x;
        
        const transform = d3.zoomIdentity
            .translate(container.clientWidth / 2 - x, container.clientHeight / 2 - y)
            .scale(1);

        svg.transition()
            .duration(750)
            .call(zoom.transform, transform);
    }

    function centerView() {
        const container = document.querySelector('.tree-container');
        const nodes = root.descendants();
        
        if (nodes.length === 0) return;

        const xExtent = d3.extent(nodes, d => d.x);
        const yExtent = d3.extent(nodes, d => d.y);

        const treeWidth = yExtent[1] - yExtent[0];
        const treeHeight = xExtent[1] - xExtent[0];

        const scale = Math.min(
            container.clientWidth * 0.8 / (treeWidth || 1),
            container.clientHeight * 0.8 / (treeHeight || 1),
            1
        ) * CONFIG.initialScale;

        const centerX = (yExtent[0] + yExtent[1]) / 2;
        const centerY = (xExtent[0] + xExtent[1]) / 2;

        const transform = d3.zoomIdentity
            .translate(container.clientWidth / 2 - centerX * scale, container.clientHeight / 2 - centerY * scale)
            .scale(scale);

        svg.transition()
            .duration(750)
            .call(zoom.transform, transform);
    }

    // =============================================
    // Radial Layout (Alternative View)
    // =============================================
    function setupRadialLayout() {
        tree = d3.tree()
            .size([2 * Math.PI, 500])
            .separation((a, b) => (a.parent === b.parent ? 1 : 2) / a.depth);

        root = d3.hierarchy(KPI_ARCHITECTURE_DATA);
        
        // Collapse initially
        root.descendants().forEach((d, i) => {
            if (d.depth > 1) {
                d._children = d.children;
                d.children = null;
            }
        });
    }

    // =============================================
    // Start Application
    // =============================================
    document.addEventListener('DOMContentLoaded', init);

})();
