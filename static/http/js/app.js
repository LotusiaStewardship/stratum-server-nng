// Lotusia Pool Dashboard - Real-time Updates
// WebSocket connection for live stats, blocks, and share updates

(function() {
    let ws = null;
    let reconnectDelay = 1000;
    const maxReconnectDelay = 30000;

    function connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/ws`;
        
        console.log('[Dashboard] Connecting to WebSocket:', wsUrl);
        ws = new WebSocket(wsUrl);

        ws.onopen = function() {
            console.log('[Dashboard] WebSocket connected');
            reconnectDelay = 1000; // Reset reconnect delay on success
            updateConnectionStatus('connected');
        };

        ws.onmessage = function(event) {
            try {
                const msg = JSON.parse(event.data);
                handleEvent(msg);
            } catch (e) {
                console.error('[Dashboard] Failed to parse event:', e);
            }
        };

        ws.onclose = function() {
            console.log('[Dashboard] WebSocket closed');
            updateConnectionStatus('disconnected');
            // Reconnect with exponential backoff
            setTimeout(function() {
                reconnectDelay = Math.min(reconnectDelay * 2, maxReconnectDelay);
                console.log('[Dashboard] Reconnecting in', reconnectDelay, 'ms');
                connect();
            }, reconnectDelay);
        };

        ws.onerror = function(error) {
            console.error('[Dashboard] WebSocket error:', error);
            updateConnectionStatus('error');
        };
    }

    function updateConnectionStatus(status) {
        const el = document.getElementById('ws-status');
        if (!el) return;
        
        switch(status) {
            case 'connected':
                el.textContent = '●';
                el.className = 'text-green-400';
                el.title = 'Live';
                break;
            case 'disconnected':
                el.textContent = '○';
                el.className = 'text-gray-500';
                el.title = 'Disconnected';
                break;
            case 'error':
                el.textContent = '●';
                el.className = 'text-red-400';
                el.title = 'Error';
                break;
            default:
                el.textContent = '●';
                el.className = 'text-yellow-400';
                el.title = 'Connecting...';
        }
    }

    function handleEvent(msg) {
        switch(msg.type) {
            case 'stats_update':
                updateStatsCards(msg);
                break;
            case 'block_found':
                handleBlockFound(msg);
                break;
            case 'share_update':
                handleShareUpdate(msg);
                break;
            default:
                console.log('[Dashboard] Unknown event type:', msg.type);
        }
    }

    function updateStatsCards(stats) {
        const el = (id) => document.getElementById(id);
        
        if (el('stat-hashrate')) {
            el('stat-hashrate').textContent = formatHashrate(stats.pool_hashrate);
        }
        if (el('stat-miners')) {
            el('stat-miners').textContent = stats.active_miners.toLocaleString();
        }
        if (el('stat-diff')) {
            el('stat-diff').textContent = formatDifficulty(stats.network_difficulty);
        }
        if (el('stat-blocks-24h')) {
            el('stat-blocks-24h').textContent = stats.blocks_found_24h.toLocaleString();
        }
    }

    function handleBlockFound(block) {
        console.log('[Dashboard] Block found:', block);
        
        // Update home page recent blocks table
        prependBlockRow('home-blocks-body', block);
        
        // Update blocks page table
        prependBlockRow('blocks-table-body', block);
        
        // Update stats cards (blocks count)
        const blocksEl = document.getElementById('stat-blocks-24h');
        if (blocksEl) {
            const current = parseInt(blocksEl.textContent.replace(/,/g, '')) || 0;
            blocksEl.textContent = (current + 1).toLocaleString();
        }
    }

    function prependBlockRow(tableId, block) {
        const table = document.getElementById(tableId);
        if (!table) return;  // Not on a page with blocks table
        
        const row = document.createElement('tr');
        row.className = 'hover:bg-primary-800 transition-colors';
        row.innerHTML = `
            <td class="px-4 py-4 text-primary-400 font-mono text-sm">${block.height}</td>
            <td class="px-4 py-4 font-mono text-xs text-gray-300">${shortHash(block.hash)}</td>
            <td class="px-4 py-4">
                <span class="px-2 py-1 rounded-full text-xs bg-green-900 text-green-300">
                    ${block.status}
                </span>
            </td>
            <td class="px-4 py-4 text-gray-300 text-sm">${escapeHtml(block.found_by)}</td>
            <td class="px-4 py-4 text-gray-500 text-sm">${formatTime(block.found_at)}</td>
        `;
        
        // Insert at the beginning
        if (table.firstChild) {
            table.insertBefore(row, table.firstChild);
        } else {
            table.appendChild(row);
        }
        
        // Limit to 50 rows
        while (table.children.length > 50) {
            table.removeChild(table.lastChild);
        }
    }

    function handleShareUpdate(update) {
        console.log('[Dashboard] Share update:', update);
        
        // Update home page workers table
        updateWorkerRow('worker-' + update.worker_id, update);
        
        // Update miners page table
        updateWorkerRow('miners-worker-' + update.worker_id, update);
    }

    function updateWorkerRow(rowId, update) {
        const row = document.getElementById(rowId);
        if (row) {
            // Update existing row
            const acceptedCell = row.querySelector('.shares-accepted');
            const rejectedCell = row.querySelector('.shares-rejected');
            const blocksCell = row.querySelector('.blocks-found');
            
            if (acceptedCell) acceptedCell.textContent = update.shares_accepted.toLocaleString();
            if (rejectedCell) rejectedCell.textContent = update.shares_rejected.toLocaleString();
            if (blocksCell) blocksCell.textContent = update.blocks_found.toLocaleString();
        }
        // Note: New workers will appear on next page refresh
        // (implementing dynamic row insertion is more complex)
    }

    function formatHashrate(h) {
        if (h >= 1e12) return (h / 1e12).toFixed(2) + ' TH/s';
        if (h >= 1e9) return (h / 1e9).toFixed(2) + ' GH/s';
        if (h >= 1e6) return (h / 1e6).toFixed(2) + ' MH/s';
        if (h >= 1e3) return (h / 1e3).toFixed(2) + ' KH/s';
        return h.toFixed(2) + ' H/s';
    }

    function formatDifficulty(d) {
        if (d >= 1e12) return (d / 1e12).toFixed(2) + ' T';
        if (d >= 1e9) return (d / 1e9).toFixed(2) + ' G';
        if (d >= 1e6) return (d / 1e6).toFixed(2) + ' M';
        return d.toFixed(2);
    }

    function shortHash(h) {
        if (h.length > 16) {
            return h.substring(0, 16) + '...';
        }
        return h;
    }

    function formatTime(isoString) {
        const date = new Date(isoString);
        return date.toLocaleString();
    }

    function escapeHtml(text) {
        const div = document.createElement('div');
        div.textContent = text;
        return div.innerHTML;
    }

    // Initialize WebSocket connection when DOM is ready
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', connect);
    } else {
        connect();
    }
})();
