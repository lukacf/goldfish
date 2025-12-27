/**
 * Goldfish Project Page JavaScript
 * Handles workspace listing, runs, and provenance graph visualization
 */

// PROJECT_ID is injected by the server in project.html
const API_VERSION = 'v1';
const API_BASE = '/project/' + PROJECT_ID + '/api/' + API_VERSION;

/**
 * Escape HTML special characters to prevent XSS
 * @param {string} text - Raw text to escape
 * @returns {string} Escaped text safe for HTML insertion
 */
function escapeHtml(text) {
    if (text === null || text === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(text);
    return div.innerHTML;
}

// State
let currentView = 'workspaces';
let currentRunsTab = 'stage';
let workspaceFilter = 'all';
let runFilter = 'all';
let graphViewMode = 'timeline'; // 'timeline' | 'cards'
let stagesViewMode = 'timeline'; // 'timeline' | 'cards'
let expandedWorkspaces = new Set(); // for timeline view
let expandedStages = new Set(); // for stages timeline view
let selectedWorkspace = null; // for cards view detail panel
let data = {
    workspaces: [],
    stageRuns: [],
    pipelineRuns: [],
    graph: null,
    stages: null,
    filteredWorkspaces: [],
    filteredRuns: []
};

// View switching
function showView(view) {
    currentView = view;

    // Update nav with ARIA attributes
    document.querySelectorAll('nav[aria-label="Main navigation"] button').forEach(btn => {
        btn.classList.remove('active');
        btn.setAttribute('aria-pressed', 'false');
    });
    event.target.classList.add('active');
    event.target.setAttribute('aria-pressed', 'true');

    // Show/hide views
    document.getElementById('view-workspaces').classList.toggle('hidden', view !== 'workspaces');
    document.getElementById('view-runs').classList.toggle('hidden', view !== 'runs');
    document.getElementById('view-stages').classList.toggle('hidden', view !== 'stages');
    document.getElementById('view-graph').classList.toggle('hidden', view !== 'graph');

    // Load data if needed
    if (view === 'workspaces' && data.workspaces.length === 0) {
        loadWorkspaces();
    } else if (view === 'runs') {
        if (currentRunsTab === 'stage' && data.stageRuns.length === 0) {
            loadStageRuns();
        } else if (currentRunsTab === 'pipeline' && data.pipelineRuns.length === 0) {
            loadPipelineRuns();
        }
    } else if (view === 'stages' && !data.stages) {
        loadStages();
    } else if (view === 'graph' && !data.graph) {
        loadGraph();
    }
}

function showRunsTab(tab) {
    currentRunsTab = tab;

    // Update tabs with ARIA attributes
    document.querySelectorAll('.tab').forEach(t => {
        t.classList.remove('active');
        t.setAttribute('aria-selected', 'false');
    });
    event.target.classList.add('active');
    event.target.setAttribute('aria-selected', 'true');

    // Show/hide content
    document.getElementById('tab-stage-runs').classList.toggle('active', tab === 'stage');
    document.getElementById('tab-pipeline-runs').classList.toggle('active', tab === 'pipeline');

    // Load data if needed
    if (tab === 'stage' && data.stageRuns.length === 0) {
        loadStageRuns();
    } else if (tab === 'pipeline' && data.pipelineRuns.length === 0) {
        loadPipelineRuns();
    }
}

// Filter functions
function setWorkspaceFilter(filter) {
    workspaceFilter = filter;

    // Update filter tags with ARIA
    document.querySelectorAll('#workspace-filters .filter-tag').forEach(tag => {
        const isActive = tag.dataset.filter === filter;
        tag.classList.toggle('active', isActive);
        tag.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    filterWorkspaces();
}

function filterWorkspaces() {
    const searchTerm = document.getElementById('workspace-search').value.toLowerCase();

    data.filteredWorkspaces = data.workspaces.filter(ws => {
        // Filter by search term
        const matchesSearch = !searchTerm ||
            ws.name.toLowerCase().includes(searchTerm) ||
            (ws.description && ws.description.toLowerCase().includes(searchTerm));

        // Filter by status
        const matchesFilter = workspaceFilter === 'all' ||
            (workspaceFilter === 'mounted' && ws.mount_status === 'mounted') ||
            (workspaceFilter === 'hibernating' && ws.mount_status === 'hibernating');

        return matchesSearch && matchesFilter;
    });

    renderWorkspaces();
}

function setRunFilter(filter) {
    runFilter = filter;

    // Update filter tags with ARIA
    document.querySelectorAll('#run-filters .filter-tag').forEach(tag => {
        const isActive = tag.dataset.filter === filter;
        tag.classList.toggle('active', isActive);
        tag.setAttribute('aria-pressed', isActive ? 'true' : 'false');
    });

    filterRuns();
}

function filterRuns() {
    const searchTerm = document.getElementById('runs-search').value.toLowerCase();

    data.filteredRuns = data.stageRuns.filter(run => {
        // Filter by search term
        const matchesSearch = !searchTerm ||
            run.workspace_name.toLowerCase().includes(searchTerm) ||
            run.stage_name.toLowerCase().includes(searchTerm) ||
            (run.pipeline_name && run.pipeline_name.toLowerCase().includes(searchTerm));

        // Filter by status
        const matchesFilter = runFilter === 'all' ||
            (runFilter === 'running' && run.status === 'running') ||
            (runFilter === 'completed' && run.status === 'completed') ||
            (runFilter === 'failed' && run.status === 'failed');

        return matchesSearch && matchesFilter;
    });

    renderStageRuns();
}

// API calls
async function loadWorkspaces() {
    const container = document.getElementById('workspaces-container');
    container.setAttribute('aria-busy', 'true');
    container.innerHTML = '<div class="loading" role="status"><div class="spinner" aria-hidden="true"></div><span>Loading workspaces...</span></div>';

    try {
        const response = await fetch(API_BASE + '/workspaces');
        const result = await response.json();
        data.workspaces = result.data || [];
        data.filteredWorkspaces = data.workspaces;
        filterWorkspaces();
    } catch (error) {
        console.error('Failed to load workspaces:', error);
        container.setAttribute('aria-busy', 'false');
        container.innerHTML =
            '<div class="empty-state" role="status"><div class="empty-state-icon" aria-hidden="true">&#x26A0;</div><p>Failed to load workspaces</p></div>';
    }
}

async function loadStageRuns() {
    const container = document.getElementById('stage-runs-container');
    container.setAttribute('aria-busy', 'true');
    container.innerHTML = '<div class="loading" role="status"><div class="spinner" aria-hidden="true"></div><span>Loading stage runs...</span></div>';

    try {
        const response = await fetch(API_BASE + '/runs?limit=100');
        const result = await response.json();
        data.stageRuns = result.data || [];
        data.filteredRuns = data.stageRuns;
        filterRuns();
    } catch (error) {
        console.error('Failed to load stage runs:', error);
        container.setAttribute('aria-busy', 'false');
        container.innerHTML =
            '<div class="empty-state" role="status"><div class="empty-state-icon" aria-hidden="true">&#x26A0;</div><p>Failed to load runs</p></div>';
    }
}

async function loadPipelineRuns() {
    const container = document.getElementById('pipeline-runs-container');
    container.setAttribute('aria-busy', 'true');
    container.innerHTML = '<div class="loading" role="status"><div class="spinner" aria-hidden="true"></div><span>Loading pipeline runs...</span></div>';

    try {
        const response = await fetch(API_BASE + '/pipelines?limit=100');
        const result = await response.json();
        data.pipelineRuns = result.data || [];
        renderPipelineRuns();
    } catch (error) {
        console.error('Failed to load pipeline runs:', error);
        container.setAttribute('aria-busy', 'false');
        container.innerHTML =
            '<div class="empty-state" role="status"><div class="empty-state-icon" aria-hidden="true">&#x26A0;</div><p>Failed to load pipelines</p></div>';
    }
}

async function loadGraph() {
    const svg = document.getElementById('graph-svg');
    if (svg) {
        svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">Loading graph...</text>';
    }

    try {
        const response = await fetch(`${API_BASE}/graph`);
        const result = await response.json();
        data.graph = result.data || {workspaces: []};
        renderGraph();
    } catch (error) {
        console.error('Failed to load graph:', error);
        if (svg) {
            svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">Failed to load graph</text>';
        }
    }
}

async function loadStages() {
    const svg = document.getElementById('stages-svg');
    if (svg) {
        svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">Loading stage versions...</text>';
    }

    try {
        const response = await fetch(`${API_BASE}/stages`);
        const result = await response.json();
        data.stages = result.data || {};
        renderStages();
    } catch (error) {
        console.error('Failed to load stages:', error);
        if (svg) {
            svg.innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">Failed to load stage versions</text>';
        }
    }
}

// Stages view mode switching
function setStagesMode(mode) {
    stagesViewMode = mode;

    // Update toggle buttons
    document.getElementById('btn-stages-timeline').classList.toggle('active', mode === 'timeline');
    document.getElementById('btn-stages-cards').classList.toggle('active', mode === 'cards');

    // Show/hide containers
    document.getElementById('stages-container-timeline').classList.toggle('hidden', mode !== 'timeline');
    document.getElementById('stages-container-cards').classList.toggle('hidden', mode !== 'cards');

    // Render the appropriate view
    renderStages();
}

function renderStages() {
    if (stagesViewMode === 'timeline') {
        renderStagesTimeline();
    } else {
        renderStagesCards();
    }
}

// Modal functions
function openModal() {
    const modal = document.getElementById('detail-modal');
    modal.classList.add('active');
    modal.setAttribute('aria-hidden', 'false');
    document.body.style.overflow = 'hidden';
    // Focus the close button for keyboard accessibility
    setTimeout(() => modal.querySelector('.modal-close').focus(), 100);
}

function closeModal() {
    const modal = document.getElementById('detail-modal');
    modal.classList.remove('active');
    modal.setAttribute('aria-hidden', 'true');
    document.body.style.overflow = '';
}

async function showWorkspaceDetails(workspaceName) {
    openModal();
    document.getElementById('modal-title').textContent = workspaceName;
    document.getElementById('modal-body').innerHTML = '<div class="loading"><div class="spinner"></div><span>Loading...</span></div>';

    try {
        const response = await fetch(API_BASE + '/workspace/' + encodeURIComponent(workspaceName));
        const result = await response.json();
        const details = result.data || {};

        const workspace = details.workspace || {};
        const versions = details.versions || [];
        const runs = details.recent_runs || [];

        let html = `
            <div class="detail-section">
                <div class="detail-section-title">Workspace Information</div>
                <div class="detail-grid">
                    <div class="detail-item">
                        <div class="detail-label">Name</div>
                        <div class="detail-value">${escapeHtml(workspace.workspace_name)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Created</div>
                        <div class="detail-value">${escapeHtml(new Date(workspace.created_at).toLocaleString())}</div>
                    </div>`;

        if (workspace.parent_workspace) {
            html += `
                    <div class="detail-item">
                        <div class="detail-label">Parent</div>
                        <div class="detail-value">${escapeHtml(workspace.parent_workspace)}</div>
                    </div>`;
        }

        if (workspace.mount_status) {
            html += `
                    <div class="detail-item">
                        <div class="detail-label">Mount Status</div>
                        <div class="detail-value">${escapeHtml(workspace.mount_status)}</div>
                    </div>`;
        }

        html += `
                </div>`;

        if (workspace.description) {
            html += `
                <div style="margin-top: 1rem;">
                    <div class="detail-label">Description</div>
                    <p style="margin-top: 0.5rem;">${escapeHtml(workspace.description)}</p>
                </div>`;
        }

        html += `
            </div>

            <div class="detail-section">
                <div class="detail-section-title">Versions (${versions.length})</div>`;

        if (versions.length > 0) {
            html += `
                <ul class="detail-list">
                    ${versions.slice(0, 10).map(v => `
                    <li>
                        <strong>${escapeHtml(v.version)}</strong>
                        <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem;">
                            Created: ${escapeHtml(new Date(v.created_at).toLocaleString())} &#x2022;
                            By: ${escapeHtml(v.created_by)}
                        </div>
                    </li>
                    `).join('')}
                    ${versions.length > 10 ? `<li style="color: var(--text-secondary); font-style: italic;">+ ${versions.length - 10} more versions</li>` : ''}
                </ul>`;
        } else {
            html += '<p style="color: var(--text-secondary);">No versions yet</p>';
        }

        html += `
            </div>

            <div class="detail-section">
                <div class="detail-section-title">Recent Runs (${runs.length})</div>`;

        if (runs.length > 0) {
            html += `
                <ul class="detail-list">
                    ${runs.slice(0, 10).map(r => `
                    <li style="cursor: pointer;" onclick="showRunDetails('${escapeHtml(r.id)}')">
                        <strong>${escapeHtml(r.stage_name)}</strong>
                        <span class="status status-${escapeHtml(r.status)}">${escapeHtml(r.status)}</span>
                        <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem;">
                            Started: ${escapeHtml(new Date(r.started_at).toLocaleString())}
                            ${r.pipeline_name ? ` &#x2022; Pipeline: ${escapeHtml(r.pipeline_name)}` : ''}
                        </div>
                    </li>
                    `).join('')}
                    ${runs.length > 10 ? `<li style="color: var(--text-secondary); font-style: italic;">+ ${runs.length - 10} more runs</li>` : ''}
                </ul>`;
        } else {
            html += '<p style="color: var(--text-secondary);">No runs yet</p>';
        }

        html += `
            </div>`;

        document.getElementById('modal-body').innerHTML = html;
    } catch (error) {
        console.error('Failed to load workspace details:', error);
        document.getElementById('modal-body').innerHTML =
            '<div class="empty-state"><div class="empty-state-icon">&#x26A0;</div><p>Failed to load details</p></div>';
    }
}

async function showRunDetails(runId) {
    openModal();
    document.getElementById('modal-title').textContent = 'Run ' + runId;
    document.getElementById('modal-body').innerHTML = '<div class="loading"><div class="spinner"></div><span>Loading...</span></div>';

    try {
        const response = await fetch(API_BASE + '/run/' + encodeURIComponent(runId));
        const result = await response.json();
        const details = result.data || {};

        const run = details.run || {};
        const signals = details.signals || [];

        const inputs = signals.filter(s => s.consumed_by === runId);
        const outputs = signals.filter(s => s.stage_run_id === runId);

        let html = `
            <div class="detail-section">
                <div class="detail-section-title">Run Information</div>
                <div class="detail-grid">
                    <div class="detail-item">
                        <div class="detail-label">Run ID</div>
                        <div class="detail-value">${escapeHtml(run.id)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Workspace</div>
                        <div class="detail-value">${escapeHtml(run.workspace_name)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Stage</div>
                        <div class="detail-value">${escapeHtml(run.stage_name)}</div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Status</div>
                        <div class="detail-value"><span class="status status-${escapeHtml(run.status)}">${escapeHtml(run.status)}</span></div>
                    </div>
                    <div class="detail-item">
                        <div class="detail-label">Started</div>
                        <div class="detail-value">${escapeHtml(new Date(run.started_at).toLocaleString())}</div>
                    </div>`;

        if (run.completed_at) {
            html += `
                    <div class="detail-item">
                        <div class="detail-label">Completed</div>
                        <div class="detail-value">${escapeHtml(new Date(run.completed_at).toLocaleString())}</div>
                    </div>`;
        }

        if (run.pipeline_name) {
            html += `
                    <div class="detail-item">
                        <div class="detail-label">Pipeline</div>
                        <div class="detail-value">${escapeHtml(run.pipeline_name)}</div>
                    </div>`;
        }

        if (run.backend_type) {
            html += `
                    <div class="detail-item">
                        <div class="detail-label">Backend</div>
                        <div class="detail-value">${escapeHtml(run.backend_type)}</div>
                    </div>`;
        }

        html += `
                </div>
            </div>

            <div class="detail-section">
                <div class="detail-section-title">Input Signals (${inputs.length})</div>`;

        if (inputs.length > 0) {
            html += `
                <ul class="detail-list">
                    ${inputs.map(s => `
                    <li>
                        <strong>${escapeHtml(s.signal_name)}</strong>
                        <span style="color: var(--text-secondary);">(${escapeHtml(s.signal_type)})</span>
                        ${s.storage_location ? `
                        <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem; word-break: break-all;">
                            ${escapeHtml(s.storage_location)}
                        </div>
                        ` : ''}
                    </li>
                    `).join('')}
                </ul>`;
        } else {
            html += '<p style="color: var(--text-secondary);">No input signals</p>';
        }

        html += `
            </div>

            <div class="detail-section">
                <div class="detail-section-title">Output Signals (${outputs.length})</div>`;

        if (outputs.length > 0) {
            html += `
                <ul class="detail-list">
                    ${outputs.map(s => `
                    <li>
                        <strong>${escapeHtml(s.signal_name)}</strong>
                        <span style="color: var(--text-secondary);">(${escapeHtml(s.signal_type)})</span>
                        ${s.storage_location ? `
                        <div style="font-size: 0.9rem; color: var(--text-secondary); margin-top: 0.25rem; word-break: break-all;">
                            ${escapeHtml(s.storage_location)}
                        </div>
                        ` : ''}
                    </li>
                    `).join('')}
                </ul>`;
        } else {
            html += '<p style="color: var(--text-secondary);">No output signals</p>';
        }

        html += `
            </div>`;

        document.getElementById('modal-body').innerHTML = html;
    } catch (error) {
        console.error('Failed to load run details:', error);
        document.getElementById('modal-body').innerHTML =
            '<div class="empty-state"><div class="empty-state-icon">&#x26A0;</div><p>Failed to load details</p></div>';
    }
}

// Close modal on background click
document.getElementById('detail-modal').addEventListener('click', function(e) {
    if (e.target.id === 'detail-modal') {
        closeModal();
    }
});

// Close modal on Escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') {
        closeModal();
    }
});

// Rendering
function renderWorkspaces() {
    const container = document.getElementById('workspaces-container');
    container.setAttribute('aria-busy', 'false');

    if (data.filteredWorkspaces.length === 0) {
        const message = data.workspaces.length === 0 ? 'No workspaces found' : 'No workspaces match the current filters';
        container.innerHTML = `<div class="empty-state" role="status"><div class="empty-state-icon" aria-hidden="true">&#x1F4C1;</div><p>${escapeHtml(message)}</p></div>`;
        return;
    }

    container.innerHTML = data.filteredWorkspaces.map(ws => {
        const safeName = escapeHtml(ws.name);
        const safeDescription = escapeHtml(ws.description);
        const safeMountStatus = escapeHtml(ws.mount_status);
        const safeVersionCount = escapeHtml(ws.version_count);
        const safePrunedCount = escapeHtml(ws.pruned_count || 0);
        const safeParentWorkspace = escapeHtml(ws.parent_workspace);

        return `
            <div class="workspace-card" role="listitem" tabindex="0"
                 onclick="showWorkspaceDetails('${safeName}')"
                 onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();showWorkspaceDetails('${safeName}');}"
                 aria-label="Workspace ${safeName}${ws.description ? ', ' + safeDescription : ''}">
                <h3>${safeName}</h3>
                ${ws.description ? `<p>${safeDescription}</p>` : ''}
                ${ws.mount_status ? `<span class="status status-${safeMountStatus}">${safeMountStatus}</span>` : ''}
                <div class="workspace-meta">
                    <div class="meta-item"><span aria-hidden="true">&#x1F4E6;</span> ${safeVersionCount} versions${ws.pruned_count > 0 ? ` <span class="pruned-badge" title="${safePrunedCount} versions hidden">(${safePrunedCount} pruned)</span>` : ''}</div>
                    ${ws.parent_workspace ? `<div class="meta-item"><span aria-hidden="true">&#x1F500;</span> from ${safeParentWorkspace}</div>` : ''}
                </div>
            </div>
        `;
    }).join('');
}

function renderStageRuns() {
    const container = document.getElementById('stage-runs-container');
    container.setAttribute('aria-busy', 'false');

    if (data.filteredRuns.length === 0) {
        const message = data.stageRuns.length === 0 ? 'No stage runs found' : 'No runs match the current filters';
        container.innerHTML = `<div class="empty-state"><div class="empty-state-icon">&#x1F3AF;</div><p>${escapeHtml(message)}</p></div>`;
        return;
    }

    container.innerHTML = '<div class="timeline">' + data.filteredRuns.map(run => {
        const safeId = escapeHtml(run.id);
        const safeWorkspaceName = escapeHtml(run.workspace_name);
        const safeStageName = escapeHtml(run.stage_name);
        const safeStatus = escapeHtml(run.status);
        const safePipelineName = escapeHtml(run.pipeline_name);

        return `
            <div class="timeline-item">
                <div class="timeline-content" style="cursor: pointer;" onclick="showRunDetails('${safeId}')">
                    <div class="timeline-time">${escapeHtml(new Date(run.started_at).toLocaleString())}</div>
                    <strong>${safeWorkspaceName}</strong> / ${safeStageName}
                    <span class="status status-${safeStatus}">${safeStatus}</span>
                    ${run.pipeline_name ? `<div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">Pipeline: ${safePipelineName}</div>` : ''}
                </div>
            </div>
        `;
    }).join('') + '</div>';
}

function renderPipelineRuns() {
    const container = document.getElementById('pipeline-runs-container');
    container.setAttribute('aria-busy', 'false');

    if (data.pipelineRuns.length === 0) {
        container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">&#x1F504;</div><p>No pipeline runs found</p></div>';
        return;
    }

    container.innerHTML = '<div class="timeline">' + data.pipelineRuns.map(run => {
        const safeWorkspaceName = escapeHtml(run.workspace_name);
        const safePipelineName = escapeHtml(run.pipeline_name || 'pipeline');
        const safeStatus = escapeHtml(run.status);
        const safeCompletedStages = escapeHtml(run.completed_stages || 0);
        const safeTotalStages = escapeHtml(run.total_stages || 0);

        return `
            <div class="timeline-item">
                <div class="timeline-content">
                    <div class="timeline-time">${escapeHtml(new Date(run.started_at).toLocaleString())}</div>
                    <strong>${safeWorkspaceName}</strong> / ${safePipelineName}
                    <span class="status status-${safeStatus}">${safeStatus}</span>
                    <div style="margin-top: 0.5rem; color: var(--text-secondary); font-size: 0.9rem;">
                        ${safeCompletedStages} / ${safeTotalStages} stages completed
                    </div>
                </div>
            </div>
        `;
    }).join('') + '</div>';
}

// D3.js graph state
let graphZoom = null;

// Color palette for workspace lanes
const LANE_COLORS = [
    '#FF6B35', // Orange (primary)
    '#4ECDC4', // Teal
    '#45B7D1', // Blue
    '#96CEB4', // Green
    '#DDA0DD', // Plum
    '#F7DC6F', // Yellow
    '#BB8FCE', // Purple
    '#85C1E9', // Light blue
];

// Graph mode switching
function setGraphMode(mode) {
    graphViewMode = mode;

    // Update toggle buttons
    document.getElementById('btn-timeline-view').classList.toggle('active', mode === 'timeline');
    document.getElementById('btn-cards-view').classList.toggle('active', mode === 'cards');

    // Show/hide containers
    document.getElementById('graph-container-timeline').classList.toggle('hidden', mode !== 'timeline');
    document.getElementById('graph-container-cards').classList.toggle('hidden', mode !== 'cards');

    // Close detail panel when switching modes
    closeDetailPanel();

    // Render the appropriate view
    renderGraph();
}

function renderGraph() {
    if (graphViewMode === 'timeline') {
        renderTimelineView();
    } else {
        renderCardsView();
    }
}

// ============================================
// Timeline View (Compressed SVG lanes)
// ============================================
function renderTimelineView() {
    const workspaces = data.graph?.workspaces || [];

    if (workspaces.length === 0) {
        document.getElementById('graph-svg').innerHTML = '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">No workspaces found</text>';
        return;
    }

    d3.select('#graph-svg').selectAll('*').remove();

    const svg = d3.select('#graph-svg');
    const container = svg.node().getBoundingClientRect();
    const width = container.width;
    const height = container.height;

    const LANE_HEIGHT = 70;
    const NODE_RADIUS = 8;
    const NODE_SPACING = 60;
    const LEFT_MARGIN = 150;
    const TOP_MARGIN = 30;
    const MAX_VISIBLE_VERSIONS = 5; // Show first 2 + last 2 + collapsed indicator

    graphZoom = d3.zoom()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });

    svg.call(graphZoom);

    const g = svg.append('g')
        .attr('transform', `translate(0, ${TOP_MARGIN})`);

    // Build workspace position map
    const wsPositions = {};
    workspaces.forEach((ws, i) => {
        wsPositions[ws.name] = {
            y: i * LANE_HEIGHT + LANE_HEIGHT / 2,
            color: LANE_COLORS[i % LANE_COLORS.length]
        };
    });

    // Draw branch lines
    workspaces.forEach(ws => {
        if (ws.parent && wsPositions[ws.parent]) {
            const parentPos = wsPositions[ws.parent];
            const childPos = wsPositions[ws.name];

            const path = d3.path();
            path.moveTo(LEFT_MARGIN - 20, parentPos.y);
            path.bezierCurveTo(
                LEFT_MARGIN + 20, parentPos.y,
                LEFT_MARGIN - 40, childPos.y,
                LEFT_MARGIN - 20, childPos.y
            );

            g.append('path')
                .attr('d', path.toString())
                .attr('fill', 'none')
                .attr('stroke', childPos.color)
                .attr('stroke-width', 2)
                .attr('stroke-dasharray', '5,3')
                .attr('opacity', 0.6);
        }
    });

    // Draw lanes
    workspaces.forEach((ws, wsIndex) => {
        const laneY = wsIndex * LANE_HEIGHT + LANE_HEIGHT / 2;
        const color = LANE_COLORS[wsIndex % LANE_COLORS.length];
        const isExpanded = expandedWorkspaces.has(ws.name);
        const versions = ws.versions || [];

        // Workspace label
        const labelGroup = g.append('g')
            .style('cursor', 'pointer')
            .on('click', () => {
                if (versions.length > MAX_VISIBLE_VERSIONS) {
                    if (isExpanded) {
                        expandedWorkspaces.delete(ws.name);
                    } else {
                        expandedWorkspaces.add(ws.name);
                    }
                    renderTimelineView();
                }
            });

        labelGroup.append('text')
            .attr('x', 10)
            .attr('y', laneY)
            .attr('dy', '0.35em')
            .attr('fill', color)
            .attr('font-weight', 'bold')
            .attr('font-size', '12px')
            .text(escapeHtml(ws.name));

        // Version count badge
        if (versions.length > 0) {
            labelGroup.append('text')
                .attr('x', 10)
                .attr('y', laneY + 14)
                .attr('fill', 'var(--text-secondary)')
                .attr('font-size', '10px')
                .text(`${versions.length} version${versions.length > 1 ? 's' : ''}`);
        }

        // Determine which versions to show
        let visibleVersions = [];
        let collapsedCount = 0;

        if (versions.length <= MAX_VISIBLE_VERSIONS || isExpanded) {
            visibleVersions = versions.map((v, i) => ({ ...v, originalIndex: i }));
        } else {
            // Show first 2, collapsed indicator, last 2
            visibleVersions = [
                { ...versions[0], originalIndex: 0 },
                { ...versions[1], originalIndex: 1 },
                { collapsed: true, count: versions.length - 4 },
                { ...versions[versions.length - 2], originalIndex: versions.length - 2 },
                { ...versions[versions.length - 1], originalIndex: versions.length - 1 }
            ];
            collapsedCount = versions.length - 4;
        }

        // Draw lane line
        const maxX = LEFT_MARGIN + (visibleVersions.length - 1) * NODE_SPACING + 50;
        g.append('line')
            .attr('x1', LEFT_MARGIN - 20)
            .attr('y1', laneY)
            .attr('x2', maxX)
            .attr('y2', laneY)
            .attr('stroke', color)
            .attr('stroke-width', 2)
            .attr('opacity', 0.3);

        // Draw version nodes
        visibleVersions.forEach((version, vi) => {
            const x = LEFT_MARGIN + vi * NODE_SPACING;

            if (version.collapsed) {
                // Collapsed indicator
                const collapseGroup = g.append('g')
                    .attr('transform', `translate(${x}, ${laneY})`)
                    .style('cursor', 'pointer')
                    .on('click', () => {
                        expandedWorkspaces.add(ws.name);
                        renderTimelineView();
                    });

                collapseGroup.append('rect')
                    .attr('x', -25)
                    .attr('y', -12)
                    .attr('width', 50)
                    .attr('height', 24)
                    .attr('rx', 12)
                    .attr('fill', 'var(--bg-tertiary)')
                    .attr('stroke', color)
                    .attr('stroke-width', 1)
                    .attr('opacity', 0.8);

                collapseGroup.append('text')
                    .attr('text-anchor', 'middle')
                    .attr('dy', '0.35em')
                    .attr('fill', 'var(--text-secondary)')
                    .attr('font-size', '10px')
                    .text(`+${version.count}`);

                return;
            }

            const nodeGroup = g.append('g')
                .attr('transform', `translate(${x}, ${laneY})`)
                .style('cursor', 'pointer');

            // Tagged versions get a golden border and slightly larger size
            const isTagged = !!version.tag_name;
            nodeGroup.append('circle')
                .attr('r', isTagged ? NODE_RADIUS + 2 : NODE_RADIUS)
                .attr('fill', color)
                .attr('stroke', isTagged ? '#FFD700' : '#fff')
                .attr('stroke-width', isTagged ? 3 : 2);

            // Version label
            nodeGroup.append('text')
                .attr('y', -14)
                .attr('text-anchor', 'middle')
                .attr('fill', 'var(--text-primary)')
                .attr('font-size', '10px')
                .attr('font-weight', 'bold')
                .text(version.version);

            // Tag label (shown below version if present)
            if (version.tag_name) {
                nodeGroup.append('text')
                    .attr('y', version.git_sha ? 30 : 18)
                    .attr('text-anchor', 'middle')
                    .attr('fill', 'var(--accent-color)')
                    .attr('font-size', '9px')
                    .attr('font-weight', 'bold')
                    .text('🏷️ ' + version.tag_name);
            }

            if (version.git_sha) {
                nodeGroup.append('text')
                    .attr('y', 18)
                    .attr('text-anchor', 'middle')
                    .attr('fill', 'var(--text-secondary)')
                    .attr('font-size', '8px')
                    .attr('font-family', 'monospace')
                    .text(version.git_sha);
            }

            // Tooltip
            nodeGroup.on('mouseover', function(event) {
                const tooltip = d3.select('body').append('div')
                    .attr('class', 'graph-tooltip')
                    .style('position', 'absolute')
                    .style('background', 'var(--bg-secondary)')
                    .style('border', '1px solid var(--border-color)')
                    .style('border-radius', '4px')
                    .style('padding', '8px 12px')
                    .style('font-size', '12px')
                    .style('pointer-events', 'none')
                    .style('z-index', '1000')
                    .style('left', (event.pageX + 10) + 'px')
                    .style('top', (event.pageY - 10) + 'px');

                tooltip.html(`
                    <strong>${escapeHtml(ws.name)} ${escapeHtml(version.version)}</strong>
                    ${version.tag_name ? `<span style="color: var(--accent-color); margin-left: 8px;">🏷️ ${escapeHtml(version.tag_name)}</span>` : ''}<br>
                    <span style="color: var(--text-secondary)">SHA:</span> ${escapeHtml(version.git_sha || 'N/A')}<br>
                    <span style="color: var(--text-secondary)">Created:</span> ${version.created_at ? new Date(version.created_at).toLocaleString() : 'N/A'}<br>
                    <span style="color: var(--text-secondary)">By:</span> ${escapeHtml(version.created_by || 'N/A')}
                    ${version.description ? '<br><span style="color: var(--text-secondary)">Note:</span> ' + escapeHtml(version.description) : ''}
                `);
            })
            .on('mouseout', () => d3.selectAll('.graph-tooltip').remove());
        });

        // Placeholder for empty workspaces
        if (versions.length === 0) {
            g.append('text')
                .attr('x', LEFT_MARGIN)
                .attr('y', laneY)
                .attr('dy', '0.35em')
                .attr('fill', 'var(--text-secondary)')
                .attr('font-size', '11px')
                .attr('font-style', 'italic')
                .text('(no versions)');
        }
    });
}

// ============================================
// Cards View (Hierarchical cards)
// ============================================
function renderCardsView() {
    const workspaces = data.graph?.workspaces || [];
    const container = document.getElementById('cards-grid');

    if (workspaces.length === 0) {
        container.innerHTML = '<div class="empty-state">No workspaces found</div>';
        return;
    }

    // Build tree structure
    const roots = [];
    const byName = {};

    workspaces.forEach(ws => {
        byName[ws.name] = { ...ws, children: [] };
    });

    workspaces.forEach(ws => {
        if (ws.parent && byName[ws.parent]) {
            byName[ws.parent].children.push(byName[ws.name]);
        } else {
            roots.push(byName[ws.name]);
        }
    });

    // Render cards recursively
    function renderCard(ws, depth = 0) {
        const versions = ws.versions || [];
        const latest = versions[versions.length - 1];
        const latestTime = latest?.created_at ? timeAgo(new Date(latest.created_at)) : 'N/A';
        const color = LANE_COLORS[Object.keys(byName).indexOf(ws.name) % LANE_COLORS.length];

        let html = `
            <div class="lineage-card ${depth > 0 ? 'child' : ''}"
                 style="border-left-color: ${color}; margin-left: ${depth * 2}rem;"
                 onclick="showVersionDetail('${escapeHtml(ws.name)}')">
                <div class="card-header">
                    <h3 class="card-title" style="color: ${color}">${escapeHtml(ws.name)}</h3>
                    ${versions.length > 0 ? `<span class="card-badge">${versions.length} versions</span>` : ''}
                </div>
                <div class="card-stats">
                    <div class="card-stat">
                        <span>Range:</span>
                        <span class="card-stat-value">
                            ${versions.length > 0 ? `${escapeHtml(versions[0].version)} → ${escapeHtml(latest.version)}` : 'none'}
                        </span>
                    </div>
                    <div class="card-stat">
                        <span>Latest:</span>
                        <span class="card-stat-value">${latestTime}</span>
                    </div>
                    ${latest?.git_sha ? `
                    <div class="card-stat">
                        <span>SHA:</span>
                        <span class="card-stat-value" style="font-family: monospace">${escapeHtml(latest.git_sha)}</span>
                    </div>
                    ` : ''}
                </div>
                ${ws.parent ? `
                <div class="card-parent">
                    Branched from <a href="#" onclick="event.stopPropagation(); showVersionDetail('${escapeHtml(ws.parent)}')">${escapeHtml(ws.parent)}</a>@${escapeHtml(ws.parent_version || '?')}
                </div>
                ` : ''}
            </div>
        `;

        // Render children
        ws.children.forEach(child => {
            html += renderCard(child, depth + 1);
        });

        return html;
    }

    container.innerHTML = roots.map(ws => renderCard(ws)).join('');
}

// ============================================
// Version Detail Panel (for Cards view)
// ============================================
function showVersionDetail(workspaceName) {
    const workspaces = data.graph?.workspaces || [];
    const ws = workspaces.find(w => w.name === workspaceName);

    if (!ws) return;

    selectedWorkspace = workspaceName;
    const panel = document.getElementById('version-detail-panel');
    const title = document.getElementById('detail-panel-title');
    const content = document.getElementById('detail-panel-content');

    title.textContent = `${workspaceName} Versions`;

    const versions = ws.versions || [];
    const prunedCount = ws.pruned_count || 0;

    let html = '';

    // Show pruned count if any
    if (prunedCount > 0) {
        html += `<div class="pruned-notice" style="color: var(--text-secondary); font-size: 0.85rem; margin-bottom: 1rem; padding: 0.5rem; background: var(--bg-secondary); border-radius: 4px;">
            <span style="opacity: 0.7">🗑️</span> ${prunedCount} version${prunedCount > 1 ? 's' : ''} pruned (hidden)
        </div>`;
    }

    if (versions.length === 0) {
        html += '<div class="empty-state">No versions yet</div>';
    } else {
        // Show versions in reverse order (newest first)
        html += [...versions].reverse().map(v => `
            <div class="version-item${v.tag_name ? ' tagged' : ''}" style="${v.tag_name ? 'border-left: 3px solid #FFD700;' : ''}">
                <div class="version-node" style="${v.tag_name ? 'background: #FFD700;' : ''}"></div>
                <div class="version-info">
                    <div class="version-header">
                        <span class="version-name">${escapeHtml(v.version)}</span>
                        ${v.tag_name ? `<span class="version-tag" style="color: #FFD700; margin-left: 8px; font-weight: bold;">🏷️ ${escapeHtml(v.tag_name)}</span>` : ''}
                        <span class="version-sha">${escapeHtml(v.git_sha || '')}</span>
                    </div>
                    <div class="version-meta">
                        ${v.created_at ? new Date(v.created_at).toLocaleString() : ''}
                        ${v.created_by ? `• ${escapeHtml(v.created_by)}` : ''}
                    </div>
                    ${v.description ? `<div class="version-desc" title="${escapeHtml(v.description)}">${escapeHtml(v.description)}</div>` : ''}
                </div>
            </div>
        `).join('');
    }

    content.innerHTML = html;

    panel.classList.remove('hidden');
    panel.classList.add('visible');
}

function closeDetailPanel() {
    const panel = document.getElementById('version-detail-panel');
    panel.classList.remove('visible');
    setTimeout(() => panel.classList.add('hidden'), 300);
    selectedWorkspace = null;
}

// ============================================
// Stage Versions Timeline View
// ============================================
function renderStagesTimeline() {
    const stages = data.stages || {};
    const workspaceNames = Object.keys(stages);

    if (workspaceNames.length === 0) {
        document.getElementById('stages-svg').innerHTML =
            '<text x="50%" y="50%" text-anchor="middle" fill="var(--text-secondary)">No stage versions found</text>';
        return;
    }

    d3.select('#stages-svg').selectAll('*').remove();

    const svg = d3.select('#stages-svg');
    const container = svg.node().getBoundingClientRect();
    const width = container.width;
    const height = container.height;

    const WORKSPACE_HEADER_HEIGHT = 40;
    const STAGE_ROW_HEIGHT = 50;
    const NODE_RADIUS = 6;
    const NODE_SPACING = 45;
    const LEFT_MARGIN = 180;
    const TOP_MARGIN = 20;
    const MAX_VISIBLE_VERSIONS = 6;

    // Calculate total height needed
    let totalHeight = TOP_MARGIN;
    workspaceNames.forEach(wsName => {
        const stageNames = Object.keys(stages[wsName] || {});
        totalHeight += WORKSPACE_HEADER_HEIGHT + (stageNames.length * STAGE_ROW_HEIGHT) + 20;
    });

    // Enable zoom
    const zoom = d3.zoom()
        .scaleExtent([0.3, 3])
        .on('zoom', (event) => {
            g.attr('transform', event.transform);
        });
    svg.call(zoom);

    const g = svg.append('g')
        .attr('transform', `translate(0, ${TOP_MARGIN})`);

    let currentY = 0;
    let wsColorIndex = 0;

    workspaceNames.forEach(wsName => {
        const wsStages = stages[wsName] || {};
        const stageNames = Object.keys(wsStages);
        const wsColor = LANE_COLORS[wsColorIndex % LANE_COLORS.length];
        wsColorIndex++;

        // Workspace header
        g.append('rect')
            .attr('x', 0)
            .attr('y', currentY)
            .attr('width', width)
            .attr('height', WORKSPACE_HEADER_HEIGHT)
            .attr('fill', wsColor)
            .attr('opacity', 0.1);

        g.append('text')
            .attr('x', 15)
            .attr('y', currentY + WORKSPACE_HEADER_HEIGHT / 2)
            .attr('dy', '0.35em')
            .attr('fill', wsColor)
            .attr('font-weight', 'bold')
            .attr('font-size', '14px')
            .text(escapeHtml(wsName));

        currentY += WORKSPACE_HEADER_HEIGHT;

        // Stage rows
        stageNames.forEach((stageName, stageIndex) => {
            const versions = wsStages[stageName] || [];
            const stageKey = `${wsName}:${stageName}`;
            const isExpanded = expandedStages.has(stageKey);
            const rowY = currentY + STAGE_ROW_HEIGHT / 2;

            // Stage label
            const labelGroup = g.append('g')
                .style('cursor', versions.length > MAX_VISIBLE_VERSIONS ? 'pointer' : 'default')
                .on('click', () => {
                    if (versions.length > MAX_VISIBLE_VERSIONS) {
                        if (isExpanded) {
                            expandedStages.delete(stageKey);
                        } else {
                            expandedStages.add(stageKey);
                        }
                        renderStagesTimeline();
                    }
                });

            labelGroup.append('text')
                .attr('x', 30)
                .attr('y', rowY)
                .attr('dy', '0.35em')
                .attr('fill', 'var(--text-primary)')
                .attr('font-size', '12px')
                .text(escapeHtml(stageName));

            labelGroup.append('text')
                .attr('x', 30)
                .attr('y', rowY + 12)
                .attr('fill', 'var(--text-secondary)')
                .attr('font-size', '9px')
                .text(`${versions.length} version${versions.length !== 1 ? 's' : ''}`);

            // Determine which versions to show
            let visibleVersions = [];
            if (versions.length <= MAX_VISIBLE_VERSIONS || isExpanded) {
                visibleVersions = versions.map((v, i) => ({ ...v, originalIndex: i }));
            } else {
                // Show first 2, collapsed indicator, last 2
                visibleVersions = [
                    { ...versions[0], originalIndex: 0 },
                    { ...versions[1], originalIndex: 1 },
                    { collapsed: true, count: versions.length - 4 },
                    { ...versions[versions.length - 2], originalIndex: versions.length - 2 },
                    { ...versions[versions.length - 1], originalIndex: versions.length - 1 }
                ];
            }

            // Draw lane line
            const maxX = LEFT_MARGIN + (visibleVersions.length - 1) * NODE_SPACING + 40;
            g.append('line')
                .attr('x1', LEFT_MARGIN - 10)
                .attr('y1', rowY)
                .attr('x2', maxX)
                .attr('y2', rowY)
                .attr('stroke', wsColor)
                .attr('stroke-width', 2)
                .attr('opacity', 0.3);

            // Draw version nodes
            visibleVersions.forEach((version, vi) => {
                const x = LEFT_MARGIN + vi * NODE_SPACING;

                if (version.collapsed) {
                    // Collapsed indicator
                    const collapseGroup = g.append('g')
                        .attr('transform', `translate(${x}, ${rowY})`)
                        .style('cursor', 'pointer')
                        .on('click', (event) => {
                            event.stopPropagation();
                            expandedStages.add(stageKey);
                            renderStagesTimeline();
                        });

                    collapseGroup.append('rect')
                        .attr('x', -18)
                        .attr('y', -10)
                        .attr('width', 36)
                        .attr('height', 20)
                        .attr('rx', 10)
                        .attr('fill', 'var(--bg-tertiary)')
                        .attr('stroke', wsColor)
                        .attr('stroke-width', 1);

                    collapseGroup.append('text')
                        .attr('text-anchor', 'middle')
                        .attr('dy', '0.35em')
                        .attr('fill', 'var(--text-secondary)')
                        .attr('font-size', '9px')
                        .text(`+${version.count}`);

                    return;
                }

                const nodeGroup = g.append('g')
                    .attr('transform', `translate(${x}, ${rowY})`)
                    .style('cursor', 'pointer');

                // Status-based color
                const statusColor = getStatusColor(version.last_run_status);

                nodeGroup.append('circle')
                    .attr('r', NODE_RADIUS)
                    .attr('fill', statusColor)
                    .attr('stroke', '#fff')
                    .attr('stroke-width', 1.5);

                // Version number label
                nodeGroup.append('text')
                    .attr('y', -12)
                    .attr('text-anchor', 'middle')
                    .attr('fill', 'var(--text-primary)')
                    .attr('font-size', '9px')
                    .attr('font-weight', 'bold')
                    .text(`v${version.version_num}`);

                // Run count badge
                if (version.run_count > 0) {
                    nodeGroup.append('text')
                        .attr('y', 14)
                        .attr('text-anchor', 'middle')
                        .attr('fill', 'var(--text-secondary)')
                        .attr('font-size', '8px')
                        .text(`${version.run_count}x`);
                }

                // Tooltip
                nodeGroup.on('mouseover', function(event) {
                    const tooltip = d3.select('body').append('div')
                        .attr('class', 'graph-tooltip')
                        .style('position', 'absolute')
                        .style('background', 'var(--bg-secondary)')
                        .style('border', '1px solid var(--border-color)')
                        .style('border-radius', '4px')
                        .style('padding', '8px 12px')
                        .style('font-size', '12px')
                        .style('pointer-events', 'none')
                        .style('z-index', '1000')
                        .style('max-width', '300px')
                        .style('left', (event.pageX + 10) + 'px')
                        .style('top', (event.pageY - 10) + 'px');

                    tooltip.html(`
                        <strong>${escapeHtml(stageName)} v${version.version_num}</strong><br>
                        <span style="color: var(--text-secondary)">Git SHA:</span> <code>${escapeHtml(version.git_sha?.substring(0, 7) || 'N/A')}</code><br>
                        <span style="color: var(--text-secondary)">Config:</span> <code>${escapeHtml(version.config_hash?.substring(0, 8) || 'N/A')}</code><br>
                        <span style="color: var(--text-secondary)">Runs:</span> ${version.run_count || 0}<br>
                        ${version.last_run_at ? `<span style="color: var(--text-secondary)">Last run:</span> ${timeAgo(new Date(version.last_run_at))}<br>` : ''}
                        ${version.last_run_status ? `<span style="color: var(--text-secondary)">Status:</span> <span class="status status-${escapeHtml(version.last_run_status)}">${escapeHtml(version.last_run_status)}</span>` : ''}
                    `);
                })
                .on('mouseout', () => d3.selectAll('.graph-tooltip').remove());
            });

            currentY += STAGE_ROW_HEIGHT;
        });

        currentY += 20; // Gap between workspaces
    });
}

function getStatusColor(status) {
    switch (status) {
        case 'completed': return '#4CAF50';
        case 'running': return '#2196F3';
        case 'failed': return '#f44336';
        case 'canceled': return '#9e9e9e';
        default: return '#757575';
    }
}

// ============================================
// Stage Versions Cards View
// ============================================
function renderStagesCards() {
    const stages = data.stages || {};
    const workspaceNames = Object.keys(stages);
    const container = document.getElementById('stages-cards-grid');

    if (workspaceNames.length === 0) {
        container.innerHTML = '<div class="empty-state">No stage versions found</div>';
        return;
    }

    let html = '';

    workspaceNames.forEach((wsName, wsIndex) => {
        const wsStages = stages[wsName] || {};
        const stageNames = Object.keys(wsStages);
        const wsColor = LANE_COLORS[wsIndex % LANE_COLORS.length];

        // Calculate workspace stats
        let totalVersions = 0;
        let totalRuns = 0;
        stageNames.forEach(stageName => {
            const versions = wsStages[stageName] || [];
            totalVersions += versions.length;
            versions.forEach(v => totalRuns += (v.run_count || 0));
        });

        html += `
            <div class="stages-workspace-card" style="border-left-color: ${wsColor};">
                <div class="stages-workspace-header">
                    <h3 style="color: ${wsColor}">${escapeHtml(wsName)}</h3>
                    <div class="stages-workspace-stats">
                        <span>${stageNames.length} stages</span>
                        <span>${totalVersions} versions</span>
                        <span>${totalRuns} runs</span>
                    </div>
                </div>
                <div class="stages-list">
        `;

        stageNames.forEach(stageName => {
            const versions = wsStages[stageName] || [];
            const latestVersion = versions[versions.length - 1];
            const latestStatus = latestVersion?.last_run_status;
            const statusClass = latestStatus ? `status-${latestStatus}` : '';

            // Calculate total runs for this stage
            const stageRuns = versions.reduce((sum, v) => sum + (v.run_count || 0), 0);

            html += `
                <div class="stage-card">
                    <div class="stage-card-header">
                        <span class="stage-name">${escapeHtml(stageName)}</span>
                        ${latestStatus ? `<span class="status ${statusClass}">${escapeHtml(latestStatus)}</span>` : ''}
                    </div>
                    <div class="stage-card-stats">
                        <div class="stage-stat">
                            <span class="stage-stat-label">Versions</span>
                            <span class="stage-stat-value">${versions.length}</span>
                        </div>
                        <div class="stage-stat">
                            <span class="stage-stat-label">Total Runs</span>
                            <span class="stage-stat-value">${stageRuns}</span>
                        </div>
                        <div class="stage-stat">
                            <span class="stage-stat-label">Latest</span>
                            <span class="stage-stat-value">v${latestVersion?.version_num || '?'}</span>
                        </div>
                    </div>
                    <div class="stage-versions-row">
                        ${versions.slice(-5).map(v => `
                            <span class="stage-version-dot ${v.last_run_status ? 'status-' + v.last_run_status : ''}"
                                  title="v${v.version_num}: ${v.run_count || 0} runs, ${v.last_run_status || 'no runs'}"></span>
                        `).join('')}
                        ${versions.length > 5 ? `<span class="stage-version-more">+${versions.length - 5}</span>` : ''}
                    </div>
                </div>
            `;
        });

        html += `
                </div>
            </div>
        `;
    });

    container.innerHTML = html;
}

// Helper: relative time
function timeAgo(date) {
    const seconds = Math.floor((new Date() - date) / 1000);
    if (seconds < 60) return 'just now';
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
    if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
    if (seconds < 604800) return `${Math.floor(seconds / 86400)}d ago`;
    return date.toLocaleDateString();
}

function resetGraphZoom() {
    if (graphZoom && graphViewMode === 'timeline') {
        d3.select('#graph-svg')
            .transition()
            .duration(750)
            .call(graphZoom.transform, d3.zoomIdentity);
    }
}

function centerGraph() {
    expandedWorkspaces.clear();
    renderGraph();
}

// Initialize
loadWorkspaces();

// Auto-refresh every 30 seconds
setInterval(() => {
    if (currentView === 'workspaces') loadWorkspaces();
    else if (currentView === 'runs' && currentRunsTab === 'stage') loadStageRuns();
    else if (currentView === 'runs' && currentRunsTab === 'pipeline') loadPipelineRuns();
    else if (currentView === 'stages') loadStages();
    else if (currentView === 'graph') loadGraph();
}, 30000);
