// KPI Architecture Tree Visualization
// Professional D3.js Interactive Tree

(function() {
    'use strict';

    // =============================================
    // Configuration
    // =============================================
    const CONFIG = {
        nodeRadius: {
            dashboard: 20,
            kpi: 16,
            metric: 14,
            derived: 14,
            root: 14,
            calculation: 12,
            external: 12,
            column: 10,
            root_column: 8,
            tag: 8,
            external_table: 10,
            external_column: 8,
            note: 6
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
        
        // Hide loading overlay
        setTimeout(() => {
            document.getElementById('loading').classList.add('hidden');
        }, 500);
        
        // Center the view
        setTimeout(centerView, 600);
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
            .attr('class', d => `node node-${d.data.type}`)
            .attr('transform', `translate(${source.y0 || 0},${source.x0 || 0})`)
            .style('opacity', 0);

        // Add circles
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

        // Add expand/collapse indicator
        nodeEnter.append('text')
            .attr('class', 'expand-indicator')
            .attr('dy', '0.35em')
            .attr('text-anchor', 'middle')
            .attr('font-size', d => Math.max(8, getNodeRadius(d.data.type) * 0.8))
            .attr('fill', '#fff')
            .attr('pointer-events', 'none')
            .text(d => d._children ? '+' : (d.children ? '−' : ''));

        // Add labels
        nodeEnter.append('text')
            .attr('class', 'label-primary')
            .attr('dy', '0.35em')
            .attr('x', d => getNodeRadius(d.data.type) + 8)
            .attr('text-anchor', 'start')
            .attr('fill', '#1e293b')
            .attr('font-size', d => d.depth === 0 ? 16 : (d.depth === 1 ? 14 : 12))
            .attr('font-weight', d => d.depth <= 1 ? 600 : 400)
            .text(d => truncateLabel(d.data.name, 35));

        // Add type badge
        nodeEnter.append('text')
            .attr('class', 'label-secondary')
            .attr('dy', '1.8em')
            .attr('x', d => getNodeRadius(d.data.type) + 8)
            .attr('text-anchor', 'start')
            .attr('fill', '#64748b')
            .attr('font-size', 10)
            .text(d => getTypeLabel(d.data));

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

        nodeUpdate.transition()
            .duration(CONFIG.animation.duration)
            .attr('transform', d => `translate(${d.y},${d.x})`)
            .style('opacity', d => isNodeVisible(d) ? 1 : 0.2);

        nodeUpdate.select('circle')
            .attr('r', d => getNodeRadius(d.data.type))
            .attr('fill', d => isNodeHighlighted(d) ? d3.color(getNodeColor(d.data.type, d.data)).brighter(0.5) : getNodeColor(d.data.type, d.data));

        nodeUpdate.select('.expand-indicator')
            .text(d => d._children ? '+' : (d.children ? '−' : ''));

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

        linkUpdate.transition()
            .duration(CONFIG.animation.duration)
            .attr('d', d => diagonal(d.source, d.target))
            .attr('stroke', d => isLinkHighlighted(d) ? '#3b82f6' : '#334155')
            .attr('stroke-width', d => isLinkHighlighted(d) ? 2 : 1.5)
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
        
        // DB status badge
        const dbStatus = d.data.dbNotFound ? 
            '<span style="background: #d97706; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;">⚠️ DB NOT FOUND</span>' : 
            (d.data.database ? '<span style="background: #059669; color: white; padding: 2px 8px; border-radius: 4px; font-size: 11px;">✓ DB MAPPED</span>' : '');

        let detailsHTML = `
            <div class="details-header" style="background: ${d.data.dbNotFound ? '#fef3c7' : '#f0fdf4'};">
                <div class="details-title">
                    <span style="font-size: 18px;">${typeConfig.icon}</span>
                    ${d.data.name}
                </div>
            </div>
            <div class="details-content">
                <div class="detail-row">
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
        
        // DB status indicator
        const dbStatusColor = d.data.dbNotFound ? '#d97706' : (d.data.database ? '#059669' : typeConfig.color);
        const dbStatusLabel = d.data.dbNotFound ? 'DB NOT FOUND' : (d.data.database ? 'DB MAPPED' : typeConfig.label);

        let content = `
            <div class="tooltip-header">
                <div class="tooltip-dot" style="background: ${dbStatusColor};"></div>
                <span class="tooltip-title">${d.data.name}</span>
                <span class="tooltip-type" style="background: ${dbStatusColor}; color: white;">
                    ${dbStatusLabel}
                </span>
            </div>
        `;

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
