/**
 * Goldfish Index Page JavaScript
 * Handles project listing and navigation
 */

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

async function loadProjects() {
    try {
        const response = await fetch('/api/v1/projects');
        const data = await response.json();

        const container = document.getElementById('projects-container');

        if (data.projects.length === 0) {
            container.innerHTML = `
                <div class="empty-state">
                    <div class="empty-state-icon" aria-hidden="true">&#x1F4C1;</div>
                    <p>No active Goldfish projects found</p>
                    <p style="margin-top: 1rem; font-size: 0.9rem;">Start a daemon to see projects here</p>
                </div>
            `;
            return;
        }

        container.setAttribute('aria-busy', 'false');
        container.innerHTML = '<div class="project-grid" role="list">' + data.projects.map(project => {
            const safeName = escapeHtml(project.name);
            const safeRoot = escapeHtml(project.root);
            const safeId = escapeHtml(project.id);
            return `
                <div class="project-card" role="listitem" tabindex="0"
                     onclick="window.location.href='/project/${safeId}/'"
                     onkeydown="if(event.key==='Enter'||event.key===' '){event.preventDefault();window.location.href='/project/${safeId}/';}"
                     aria-label="Project ${safeName}, located at ${safeRoot}">
                    <h2>${safeName}</h2>
                    <p>${safeRoot}</p>
                </div>
            `;
        }).join('') + '</div>';

    } catch (error) {
        console.error('Failed to load projects:', error);
        document.getElementById('projects-container').innerHTML = `
            <div class="empty-state">
                <div class="empty-state-icon" aria-hidden="true">&#x26A0;</div>
                <p>Failed to load projects</p>
            </div>
        `;
    }
}

// Load projects on page load
loadProjects();

// Refresh project list every 10 seconds
setInterval(loadProjects, 10000);
