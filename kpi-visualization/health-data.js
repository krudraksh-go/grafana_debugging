// Mock Health Data for KPI Architecture
// This simulates real-time health monitoring data

const HEALTH_THRESHOLDS = {
    // Define thresholds for each metric type
    "UPH": { min: 100, max: null, unit: "units/hr", target: 150 },
    "Picks per tote": { min: 3, max: 10, unit: "picks", target: 5 },
    "TPH all PPS": { min: 80, max: null, unit: "totes/hr", target: 120 },
    "Operator pull TPH": { min: 60, max: null, unit: "totes/hr", target: 100 },
    "OWT per unit": { min: null, max: 45, unit: "sec", target: 30 },
    "PPF": { min: 2, max: null, unit: "picks/face", target: 4 },
    "VTM TPH all VTMs": { min: 40, max: null, unit: "totes/hr", target: 60 },
    "Effective VTMs": { min: 5, max: null, unit: "count", target: 8 },
    "# of working VTMs": { min: 6, max: null, unit: "count", target: 10 },
    "Cycle time": { min: null, max: 180, unit: "sec", target: 120 },
    "Horizontal movement": { min: null, max: 60, unit: "sec", target: 40 },
    "VTM nav factor": { min: 0.7, max: null, unit: "ratio", target: 0.85 },
    "Fetch / drop": { min: null, max: 25, unit: "sec", target: 15 },
    "HTM TPH all HTMs": { min: 50, max: null, unit: "totes/hr", target: 80 },
    "Avg. relay position occupancy": { min: null, max: 85, unit: "%", target: 70 },
    "OWT": { min: null, max: 40, unit: "sec", target: 25 },
    "Nav factor": { min: 0.75, max: null, unit: "ratio", target: 0.9 },
};

// Mock current values - some intentionally bad to show alerts
const MOCK_HEALTH_DATA = {
    "UPH": { 
        value: 85, 
        status: "critical",
        trend: "down",
        lastUpdated: "2 min ago"
    },
    "Picks per tote": { 
        value: 4.2, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "1 min ago"
    },
    "TPH all PPS": { 
        value: 72, 
        status: "warning",
        trend: "down",
        lastUpdated: "30 sec ago"
    },
    "Operator pull TPH": { 
        value: 55, 
        status: "critical",
        trend: "down",
        lastUpdated: "1 min ago"
    },
    "OWT per unit": { 
        value: 52, 
        status: "critical",
        trend: "up",
        lastUpdated: "45 sec ago"
    },
    "PPF": { 
        value: 3.8, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "2 min ago"
    },
    "# of logged in PPS": { 
        value: 8, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "5 min ago"
    },
    "VTM TPH all VTMs": { 
        value: 35, 
        status: "critical",
        trend: "down",
        lastUpdated: "1 min ago"
    },
    "Effective VTMs": { 
        value: 4, 
        status: "critical",
        trend: "down",
        lastUpdated: "2 min ago"
    },
    "# of working VTMs": { 
        value: 5, 
        status: "warning",
        trend: "down",
        lastUpdated: "3 min ago"
    },
    "Blocked due to relay position unavailability": { 
        value: 12, 
        status: "critical",
        trend: "up",
        lastUpdated: "1 min ago"
    },
    "Cycle time": { 
        value: 195, 
        status: "critical",
        trend: "up",
        lastUpdated: "30 sec ago"
    },
    "Horizontal movement": { 
        value: 75, 
        status: "critical",
        trend: "up",
        lastUpdated: "1 min ago"
    },
    "VTM nav factor": { 
        value: 0.62, 
        status: "critical",
        trend: "down",
        lastUpdated: "2 min ago"
    },
    "% aisle changes": { 
        value: 45, 
        status: "warning",
        trend: "up",
        lastUpdated: "5 min ago"
    },
    "Fetch / drop": { 
        value: 28, 
        status: "warning",
        trend: "up",
        lastUpdated: "2 min ago"
    },
    "HTM TPH all HTMs": { 
        value: 65, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "1 min ago"
    },
    "Avg. relay position occupancy": { 
        value: 92, 
        status: "critical",
        trend: "up",
        lastUpdated: "30 sec ago"
    },
    "OWT": { 
        value: 48, 
        status: "critical",
        trend: "up",
        lastUpdated: "1 min ago"
    },
    "Nav factor": { 
        value: 0.78, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "3 min ago"
    },
    "Storable to relay TPH": { 
        value: 42, 
        status: "healthy",
        trend: "stable",
        lastUpdated: "2 min ago"
    },
    "Relay to storable TPH": { 
        value: 38, 
        status: "warning",
        trend: "down",
        lastUpdated: "1 min ago"
    },
    "Orderlines per tote": {
        value: 3.2,
        status: "healthy",
        trend: "stable",
        lastUpdated: "2 min ago"
    },
    "Quantity per orderline": {
        value: 1.8,
        status: "healthy",
        trend: "stable",
        lastUpdated: "3 min ago"
    },
    "Number of PPS": {
        value: 12,
        status: "healthy",
        trend: "stable",
        lastUpdated: "10 min ago"
    },
    "TPH / VTM": {
        value: 8.5,
        status: "warning",
        trend: "down",
        lastUpdated: "2 min ago"
    },
    "TPH / HTM": {
        value: 11.2,
        status: "healthy",
        trend: "stable",
        lastUpdated: "1 min ago"
    }
};

// Critical paths - chains of issues from root to leaf
const CRITICAL_PATHS = [
    {
        id: 1,
        severity: "critical",
        title: "UPH Drop - VTM Navigation Issue",
        description: "High cycle time due to poor VTM navigation factor",
        path: ["UPH", "TPH all PPS", "VTM TPH all VTMs", "TPH / VTM", "Cycle time", "Horizontal movement", "VTM nav factor"],
        rootCause: "VTM nav factor at 0.62 (target: 0.85)",
        impact: "UPH reduced by 35%",
        recommendation: "Check grid layout and VTM pathfinding algorithms"
    },
    {
        id: 2,
        severity: "critical", 
        title: "Relay Position Bottleneck",
        description: "Relay positions over-occupied causing VTM blocking",
        path: ["UPH", "TPH all PPS", "VTM TPH all VTMs", "Effective VTMs", "Breakup #1", "# of working VTMs", "Work assigned", "Blocked due to relay position unavailability"],
        rootCause: "Relay occupancy at 92% (target: 70%)",
        impact: "VTM efficiency reduced by 40%",
        recommendation: "Increase back-to-store priority or add relay positions"
    },
    {
        id: 3,
        severity: "critical",
        title: "High OWT Impact",
        description: "Operator working time exceeding threshold",
        path: ["UPH", "TPH all PPS", "Operator pull TPH", "OWT per unit"],
        rootCause: "OWT at 52 sec (target: 30 sec)",
        impact: "Operator TPH down 45%",
        recommendation: "Review pick face ergonomics and operator training"
    },
    {
        id: 4,
        severity: "warning",
        title: "Aisle Change Inefficiency",
        description: "High percentage of aisle changes affecting travel time",
        path: ["UPH", "TPH all PPS", "VTM TPH all VTMs", "TPH / VTM", "Cycle time", "Horizontal movement", "Distance", "% aisle changes"],
        rootCause: "Aisle changes at 45% (should be <30%)",
        impact: "Travel time increased by 25%",
        recommendation: "Optimize slotting strategy to reduce cross-aisle picks"
    }
];

// Health status colors - Muted Industrial Palette
const HEALTH_COLORS = {
    critical: "#b85450",  // Muted Red
    warning: "#c4824a",   // Muted Orange
    healthy: "#48a999",   // Muted Teal
    unknown: "#718096"    // Gray
};

// Get health data for a node
function getNodeHealth(nodeName) {
    return MOCK_HEALTH_DATA[nodeName] || null;
}

// Get threshold for a node
function getNodeThreshold(nodeName) {
    return HEALTH_THRESHOLDS[nodeName] || null;
}

// Check if node is in any critical path
function isNodeInCriticalPath(nodeName) {
    for (const path of CRITICAL_PATHS) {
        if (path.path.includes(nodeName)) {
            return path;
        }
    }
    return null;
}

// Get all critical paths containing a node
function getCriticalPathsForNode(nodeName) {
    return CRITICAL_PATHS.filter(path => path.path.includes(nodeName));
}

// Get health summary
function getHealthSummary() {
    let critical = 0, warning = 0, healthy = 0;
    
    for (const [name, data] of Object.entries(MOCK_HEALTH_DATA)) {
        if (data.status === "critical") critical++;
        else if (data.status === "warning") warning++;
        else healthy++;
    }
    
    return { critical, warning, healthy, total: critical + warning + healthy };
}

// Export for use
if (typeof module !== 'undefined' && module.exports) {
    module.exports = { 
        MOCK_HEALTH_DATA, 
        HEALTH_THRESHOLDS, 
        CRITICAL_PATHS, 
        HEALTH_COLORS,
        getNodeHealth,
        getNodeThreshold,
        isNodeInCriticalPath,
        getCriticalPathsForNode,
        getHealthSummary
    };
}
