const socket = io();
let isConnected = false;
let stockData = [];
let etfData = [];

// Connection status handling
socket.on('connect', function () {
    console.log('[INFO] Connected to server');
    isConnected = true;
    updateStatus('Đã kết nối - Đang chờ dữ liệu...', 'connected');
    
    // Request initial data when connected
    console.log('[INFO] Requesting initial stock data...');
});

socket.on('disconnect', function () {
    console.log('[INFO] Disconnected from server');
    isConnected = false;
    updateStatus('Mất kết nối - Đang thử kết nối lại...', 'disconnected');
});

function updateStatus(message, className) {
    const statusDiv = document.getElementById('status');
    if (statusDiv) {
        statusDiv.textContent = message;
        statusDiv.className = `status ${className}`;
    }
}

// Stock data handling
socket.on("stock_data", function (data) {
    console.log("[DEBUG] Received stock data:", data);
    
    // Validate data structure
    if (!Array.isArray(data)) {
        console.error('[ERROR] Invalid stock data format:', data);
        return;
    }
    
    stockData = data;
    updateStockTable(data, 'stock');
    updateConnectionStatus();
});

// ETF data handling
socket.on("etf_data", function (data) {
    console.log("[DEBUG] Received ETF data:", data);
    
    // Validate data structure
    if (!Array.isArray(data)) {
        console.error('[ERROR] Invalid ETF data format:', data);
        return;
    }
    
    etfData = data;
    updateETFTable(data);
    // Update ETF index-cards in Home section
    data.forEach(etf => {
        const card = document.getElementById(`etf-${etf.symbol}`);
        if (!card) return;

        const valueDiv = card.querySelector(".index-value");
        const changeDiv = card.querySelector(".index-change");

        if (valueDiv && !isNaN(parseFloat(etf.price))) {
            valueDiv.textContent = parseFloat(etf.price).toLocaleString("en-US", {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2,
            });
        }

        if (changeDiv && !isNaN(parseFloat(etf.change))) {
            const change = parseFloat(etf.change);
            changeDiv.textContent = `${change >= 0 ? "↗ +" : "↘ "}${change.toFixed(2)}%`;
            changeDiv.className = "index-change " + (change > 0 ? "positive" : change < 0 ? "negative" : "neutral");
        }
    });

    updateConnectionStatus();
});

function updateStockTable(data, dataType = 'stock') {
    const tbody = document.querySelector("#stockTable tbody");
    const loadingDiv = document.getElementById("loading");
    const stockTable = document.getElementById("stockTable");

    if (!tbody || !loadingDiv || !stockTable) {
        console.error('[ERROR] Required DOM elements not found');
        return;
    }

    // Hide loading and show table
    loadingDiv.style.display = "none";
    stockTable.style.display = "table";

    // Clear old rows
    tbody.innerHTML = "";

    // Check if data is valid
    if (!Array.isArray(data) || data.length === 0) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 3;
        cell.textContent = "Không có dữ liệu";
        cell.style.textAlign = "center";
        cell.style.color = "#666";
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }

    data.forEach(stock => {
        // Validate individual stock data
        if (!stock || typeof stock !== 'object') {
            console.warn('[WARNING] Invalid stock object:', stock);
            return;
        }

        const row = document.createElement("tr");

        // Symbol cell
        const symbolCell = document.createElement("td");
        symbolCell.className = "symbol-cell";
        symbolCell.textContent = stock.symbol || "N/A";

        // Price cell with proper formatting
        const priceCell = document.createElement("td");
        priceCell.className = "price-cell";

        let numericPrice = parseFloat(stock.price);
        if (isNaN(numericPrice) || numericPrice === null || numericPrice === undefined) {
            priceCell.textContent = "N/A";
        } else {
            // Format price with comma separator and 2 decimal places
            const formattedPrice = numericPrice.toLocaleString('en-US', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
            priceCell.textContent = `US$ ${formattedPrice}`;
        }

        // Change cell
        const changeCell = document.createElement("td");
        let numericChange = parseFloat(stock.change);

        if (isNaN(numericChange) || numericChange === null || numericChange === undefined) {
            changeCell.textContent = "N/A";
            changeCell.className = "neutral";
        } else {
            changeCell.textContent = numericChange.toFixed(2) + "%";

            if (numericChange > 0) {
                changeCell.className = "up";
                changeCell.textContent = "+" + changeCell.textContent;
            } else if (numericChange < 0) {
                changeCell.className = "down";
            } else {
                changeCell.className = "neutral";
            }
        }

        // Add cells to row
        row.appendChild(symbolCell);
        row.appendChild(priceCell);
        row.appendChild(changeCell);
        tbody.appendChild(row);
    });
}

function updateETFTable(data) {
    const tbody = document.querySelector("#etfTable tbody");
    const etfTable = document.getElementById("etfTable");

    if (!tbody || !etfTable) {
        console.error('[ERROR] ETF table elements not found');
        return;
    }

    // Show ETF table
    etfTable.style.display = "table";

    // Clear old rows
    tbody.innerHTML = "";

    // Check if data is valid
    if (!Array.isArray(data) || data.length === 0) {
        const row = document.createElement("tr");
        const cell = document.createElement("td");
        cell.colSpan = 3;
        cell.textContent = "Không có dữ liệu ETF";
        cell.style.textAlign = "center";
        cell.style.color = "#666";
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }

    data.forEach(etf => {
        // Validate individual ETF data
        if (!etf || typeof etf !== 'object') {
            console.warn('[WARNING] Invalid ETF object:', etf);
            return;
        }

        const row = document.createElement("tr");

        // Name cell (for ETFs, we show the full name)
        const nameCell = document.createElement("td");
        nameCell.className = "symbol-cell";
        nameCell.innerHTML = `
            <div>
                <strong>${etf.name || "N/A"}</strong><br>
                <small style="color: #666;">${etf.symbol || "N/A"}</small>
            </div>
        `;

        // Price cell
        const priceCell = document.createElement("td");
        priceCell.className = "price-cell";

        let numericPrice = parseFloat(etf.price);
        if (isNaN(numericPrice) || numericPrice === null || numericPrice === undefined) {
            priceCell.textContent = "N/A";
        } else {
            const formattedPrice = numericPrice.toLocaleString('en-US', {
                minimumFractionDigits: 2,
                maximumFractionDigits: 2
            });
            priceCell.textContent = `US$ ${formattedPrice}`;
        }

        // Change cell
        const changeCell = document.createElement("td");
        let numericChange = parseFloat(etf.change);

        if (isNaN(numericChange) || numericChange === null || numericChange === undefined) {
            changeCell.textContent = "N/A";
            changeCell.className = "neutral";
        } else {
            changeCell.textContent = numericChange.toFixed(2) + "%";

            if (numericChange > 0) {
                changeCell.className = "up";
                changeCell.textContent = "+" + changeCell.textContent;
            } else if (numericChange < 0) {
                changeCell.className = "down";
            } else {
                changeCell.className = "neutral";
            }
        }

        row.appendChild(nameCell);
        row.appendChild(priceCell);
        row.appendChild(changeCell);
        tbody.appendChild(row);
    });
}

function updateConnectionStatus() {
    const now = new Date();
    const timeString = now.toLocaleTimeString('vi-VN');
    updateStatus(`Cập nhật lúc: ${timeString}`, 'connected');
}

// Error handling
socket.on('connect_error', function (error) {
    console.error('[ERROR] Connection failed:', error);
    updateStatus('Lỗi kết nối - Vui lòng thử lại', 'disconnected');
});

socket.on('error', function (error) {
    console.error('[ERROR] Socket error:', error);
    updateStatus('Lỗi socket - Kiểm tra kết nối', 'disconnected');
});

// Fallback: Fetch data via REST API if WebSocket fails
async function fetchStockDataFallback() {
    try {
        console.log('[INFO] Fetching stock data via REST API fallback...');
        
        const response = await fetch('/api/quotes', {
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        const data = await response.json();
        console.log('[INFO] Fallback data received:', data);

        if (Array.isArray(data)) {
            // Separate stocks and ETFs
            const stocks = data.filter(item => item.type === 'stock');
            const etfs = data.filter(item => item.type === 'etf');

            if (stocks.length > 0) {
                updateStockTable(stocks);
            }
            if (etfs.length > 0) {
                updateETFTable(etfs);
            }

            updateConnectionStatus();
        }

    } catch (error) {
        console.error('[ERROR] Fallback API failed:', error);
        updateStatus('Lỗi tải dữ liệu - Đang thử lại...', 'disconnected');
    }
}

// Retry connection every 5 seconds if disconnected
setInterval(function () {
    if (!isConnected && !socket.connected) {
        console.log('[INFO] Attempting to reconnect...');
        socket.connect();
    }
}, 5000);

// Use fallback API every 30 seconds if no socket data
let lastDataTime = Date.now();
socket.on('stock_data', () => { lastDataTime = Date.now(); });
socket.on('etf_data', () => { lastDataTime = Date.now(); });

setInterval(function () {
    const timeSinceLastData = Date.now() - lastDataTime;
    if (timeSinceLastData > 30000) { // 30 seconds
        console.log('[INFO] No recent socket data, using fallback...');
        fetchStockDataFallback();
    }
}, 30000);

// Initial fallback call when page loads
document.addEventListener('DOMContentLoaded', () => {
    console.log('[INFO] Page loaded, setting up stock tracker...');
    
    // Try fallback after 5 seconds if no socket connection
    setTimeout(() => {
        if (!isConnected) {
            fetchStockDataFallback();
        }
    }, 5000);
});

// News handling functions
let newsData = []; // Store original news data

function initializeNewsFeatures() {
    const searchInput = document.querySelector('.news-search');
    const filterSelect = document.querySelector('.news-filter');
    
    if (searchInput) {
        searchInput.addEventListener('input', debounce(() => {
            filterAndDisplayNews();
        }, 300));
    }
    
    if (filterSelect) {
        filterSelect.addEventListener('change', () => {
            filterAndDisplayNews();
        });
    }
}

function filterAndDisplayNews() {
    const searchInput = document.querySelector('.news-search');
    const filterSelect = document.querySelector('.news-filter');
    
    if (!searchInput || !filterSelect) return;
    
    const searchTerm = searchInput.value.toLowerCase();
    const category = filterSelect.value;
    
    let filteredNews = newsData;
    
    // Apply search filter
    if (searchTerm) {
        filteredNews = filteredNews.filter(news => 
            news.title.toLowerCase().includes(searchTerm) || 
            news.description.toLowerCase().includes(searchTerm) ||
            news.symbol.toLowerCase().includes(searchTerm)
        );
    }
    
    // Apply category filter if not 'all'
    if (category !== 'all') {
        filteredNews = filteredNews.filter(news => {
            switch(category) {
                case 'market':
                    return news.title.toLowerCase().includes('market') || 
                           news.description.toLowerCase().includes('market');
                case 'company':
                    return news.title.toLowerCase().includes('company') || 
                           news.description.toLowerCase().includes('company');
                case 'economy':
                    return news.title.toLowerCase().includes('economy') || 
                           news.description.toLowerCase().includes('economy');
                default:
                    return true;
            }
        });
    }
    
    displayNews(filteredNews);
}

function displayNews(news) {
    const newsGrid = document.querySelector('.news-grid');
    if (!newsGrid) return;
    
    newsGrid.innerHTML = '';
    
    if (!news || news.length === 0) {
        newsGrid.innerHTML = '<div class="no-news">No news found</div>';
        return;
    }
    
    news.forEach(item => {
        const newsItem = document.createElement('div');
        newsItem.className = 'news-item';
        
        const date = new Date(item.publishedAt);
        const formattedDate = date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
        
        newsItem.innerHTML = `
            <div class="news-symbol">${item.symbol}</div>
            <h3 class="news-title">
                <a href="${item.url}" target="_blank" rel="noopener noreferrer">
                    ${item.title}
                </a>
            </h3>
            <p class="news-description">${item.description || 'No description available'}</p>
            <div class="news-meta">
                <span>${formattedDate}</span>
                <span>${item.source}</span>
            </div>
        `;
        
        newsGrid.appendChild(newsItem);
    });
}

// Debounce function to limit how often a function is called
function debounce(func, wait) {
    let timeout;
    return function executedFunction(...args) {
        const later = () => {
            clearTimeout(timeout);
            func(...args);
        };
        clearTimeout(timeout);
        timeout = setTimeout(later, wait);
    };
}

// Modified fetchNews function
async function fetchNews() {
    const loadingElement = document.getElementById('loading-news');
    const errorElement = document.getElementById('error');
    const successElement = document.getElementById('success');
    
    if (loadingElement) loadingElement.style.display = 'block';
    if (errorElement) errorElement.style.display = 'none';
    if (successElement) successElement.style.display = 'none';
    
    try {
        const response = await fetch('/api/news');
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();
        
        // Convert the object of arrays into a single array of news
        let allNews = [];
        for (const [symbol, articles] of Object.entries(data)) {
            // Add symbol to each article
            const symbolArticles = articles.map(article => ({
                ...article,
                symbol: symbol
            }));
            allNews = allNews.concat(symbolArticles);
        }
        
        // Sort by publishedAt date
        allNews.sort((a, b) => new Date(b.publishedAt) - new Date(a.publishedAt));
        
        newsData = allNews; // Store the original data
        filterAndDisplayNews(); // Display with current filters
        
        if (successElement) {
            successElement.style.display = 'block';
            setTimeout(() => {
                successElement.style.display = 'none';
            }, 3000);
        }
    } catch (error) {
        console.error('Error fetching news:', error);
        if (errorElement) {
            errorElement.textContent = 'Error loading news. Please try again.';
            errorElement.style.display = 'block';
        }
    } finally {
        if (loadingElement) loadingElement.style.display = 'none';
    }
}

// Initialize news features when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    initializeNewsFeatures();
    fetchNews(); // Initial news fetch
});