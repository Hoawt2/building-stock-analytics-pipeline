let isLoading = false;

async function fetchNews() {
    if (isLoading) return;

    const container = document.getElementById('news-container');
    const loading = document.getElementById('loading');
    const error = document.getElementById('error');
    const success = document.getElementById('success');

    try {
        isLoading = true;

        // Show loading state
        loading.style.display = 'block';
        error.style.display = 'none';
        success.style.display = 'none';
        container.innerHTML = '';

        console.log('[INFO] Fetching news data...');
        
        // Add timeout and proper error handling
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 30000); // 30 second timeout
        
        const res = await fetch('/api/news', {
            signal: controller.signal,
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });
        
        clearTimeout(timeoutId);

        if (!res.ok) {
            throw new Error(`HTTP ${res.status}: ${res.statusText}`);
        }

        const data = await res.json();
        console.log('[INFO] News data received:', data);

        // Validate response structure
        if (!data || typeof data !== 'object') {
            throw new Error('Invalid response format');
        }

        // Hide loading
        loading.style.display = 'none';
        success.style.display = 'block';

        // Hide success message after 3 seconds
        setTimeout(() => {
            success.style.display = 'none';
        }, 3000);

        let hasNews = false;

        for (const [symbol, articles] of Object.entries(data)) {
            if (!articles || !Array.isArray(articles) || articles.length === 0) {
                console.log(`[INFO] No articles for ${symbol}`);
                continue;
            }

            hasNews = true;
            const group = document.createElement('div');
            group.className = 'symbol-group';

            const header = document.createElement('h2');
            header.innerHTML = `
                <span class="symbol-badge">${escapeHtml(symbol)}</span>
                <span>${escapeHtml(getCompanyName(symbol))}</span>
            `;
            group.appendChild(header);

            const articlesContainer = document.createElement('div');
            articlesContainer.className = 'articles-grid';

            articles.forEach((article, index) => {
                // Validate article structure
                if (!article || typeof article !== 'object') {
                    console.warn(`[WARNING] Invalid article structure for ${symbol}:`, article);
                    return;
                }

                const articleDiv = document.createElement('div');
                articleDiv.className = 'article';
                articleDiv.style.animationDelay = `${index * 0.1}s`;

                const title = article.title || 'Tiêu đề không có sẵn';
                const description = article.description || 'Mô tả không có sẵn';
                const url = article.url || '#';
                const publishedAt = article.publishedAt ? 
                    formatDate(article.publishedAt) : 'Thời gian không xác định';
                const source = article.source || 'Nguồn không xác định';

                // Validate URL
                const safeUrl = isValidUrl(url) ? url : '#';

                articleDiv.innerHTML = `
                    <a href="${escapeHtml(safeUrl)}" target="_blank" class="article-title" rel="noopener noreferrer">
                        ${escapeHtml(title)}
                    </a>
                    <p class="article-description">${escapeHtml(description)}</p>
                    <div class="article-meta">
                        <span class="article-source">${escapeHtml(source)}</span>
                        <small>${escapeHtml(publishedAt)}</small>
                    </div>
                `;

                articlesContainer.appendChild(articleDiv);
            });

            group.appendChild(articlesContainer);
            container.appendChild(group);
        }

        if (!hasNews) {
            container.innerHTML = '<div class="no-news">📰 Không có tin tức nào để hiển thị.</div>';
        }

    } catch (e) {
        console.error('[ERROR] Failed to fetch news:', e);
        loading.style.display = 'none';
        error.style.display = 'block';
        success.style.display = 'none';

        let errorMessage = 'Lỗi không xác định';
        
        if (e.name === 'AbortError') {
            errorMessage = 'Timeout - Yêu cầu mất quá nhiều thời gian';
        } else if (e.message.includes('Failed to fetch')) {
            errorMessage = 'Không thể kết nối đến server';
        } else {
            errorMessage = e.message;
        }

        container.innerHTML = `
            <div class="no-news">
                ❌ Lỗi khi tải tin tức: ${escapeHtml(errorMessage)}<br>
                <button onclick="fetchNews()" style="margin-top: 10px; padding: 8px 16px; border: none; border-radius: 5px; background: #667eea; color: white; cursor: pointer;">
                    Thử lại
                </button>
            </div>
        `;
    } finally {
        isLoading = false;
    }
}

function getCompanyName(symbol) {
    const names = {
        'AAPL': 'Apple Inc.',
        'GOOGL': 'Alphabet Inc.',
        'MSFT': 'Microsoft Corporation',
        'NVDA': 'NVIDIA Corporation',
        'TSLA': 'Tesla Inc.'
    };
    return names[symbol] || symbol;
}

// Utility functions for security and validation
function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function isValidUrl(string) {
    try {
        new URL(string);
        return true;
    } catch (_) {
        return false;
    }
}

function formatDate(dateString) {
    try {
        const date = new Date(dateString);
        if (isNaN(date.getTime())) {
            return 'Thời gian không hợp lệ';
        }
        return date.toLocaleString('vi-VN', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch (e) {
        console.warn('[WARNING] Error formatting date:', e);
        return 'Thời gian không hợp lệ';
    }
}

// Enhanced error handling for page load
document.addEventListener('DOMContentLoaded', () => {
    console.log('[INFO] Page loaded, fetching initial news...');
    fetchNews();
});

// Auto refresh every 5 minutes with error handling
setInterval(() => {
    if (!isLoading) {
        console.log('[INFO] Auto-refreshing news...');
        fetchNews();
    }
}, 5 * 60 * 1000);