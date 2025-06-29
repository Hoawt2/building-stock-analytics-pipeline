// Chatbot Widget Controller
class ChatbotWidget {
    constructor() {
        this.isOpen = false;
        this.init();
    }

    init() {
        this.bindEvents();
        this.addWelcomeMessage();
    }

    bindEvents() {
        const chatbotIcon = document.getElementById('chatbotIcon');
        const closeChatBtn = document.getElementById('closeChatBtn');
        const sendButton = document.getElementById('sendButton');
        const chatInput = document.getElementById('chatInput');

        if (chatbotIcon) {
            chatbotIcon.addEventListener('click', () => this.toggleChat());
        }

        if (closeChatBtn) {
            closeChatBtn.addEventListener('click', () => this.closeChat());
        }

        if (sendButton) {
            sendButton.addEventListener('click', () => this.sendMessage());
        }

        if (chatInput) {
            chatInput.addEventListener('keypress', (e) => {
                if (e.key === 'Enter') {
                    this.sendMessage();
                }
            });
        }
    }

    toggleChat() {
        const chatWindow = document.getElementById('chatbotWindow');
        if (!chatWindow) return;

        if (this.isOpen) {
            this.closeChat();
        } else {
            this.openChat();
        }
    }

    openChat() {
        const chatWindow = document.getElementById('chatbotWindow');
        const chatbotIcon = document.getElementById('chatbotIcon');
        
        if (chatWindow && chatbotIcon) {
            chatWindow.classList.add('show');
            chatbotIcon.style.animation = 'none';
            this.isOpen = true;
            
            // Focus vào input
            setTimeout(() => {
                const chatInput = document.getElementById('chatInput');
                if (chatInput) chatInput.focus();
            }, 300);
        }
    }

    closeChat() {
        const chatWindow = document.getElementById('chatbotWindow');
        const chatbotIcon = document.getElementById('chatbotIcon');
        
        if (chatWindow && chatbotIcon) {
            chatWindow.classList.remove('show');
            chatbotIcon.style.animation = 'bounce 2s infinite';
            this.isOpen = false;
        }
    }

    addWelcomeMessage() {
        setTimeout(() => {
            this.addMessage('Xin chào! Tôi là chuyên gia tài chính với 20 năm kinh nghiệm trong lĩnh vực đầu tư chứng khoán Mỹ. Bạn có thể hỏi tôi về phân tích các mã như AAPL, GOOGL, MSFT hoặc bất kỳ câu hỏi đầu tư nào! 📈💼', 'ai');
        }, 1000);
    }

    async sendMessage() {
        const chatInput = document.getElementById('chatInput');
        const sendButton = document.getElementById('sendButton');
        
        if (!chatInput || !sendButton) return;

        const message = chatInput.value.trim();
        if (!message) return;

        // Disable input
        chatInput.disabled = true;
        sendButton.disabled = true;

        // Add user message
        this.addMessage(message, 'user');
        chatInput.value = '';

        // Add loading message
        const loadingDiv = this.addMessage('<div class="loading"></div>', 'ai');

        try {
            const response = await fetch('/api/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ question: message })
            });

            const data = await response.json();

            // Remove loading
            if (loadingDiv) loadingDiv.remove();

            // Add AI response
            if (data.response) {
                this.addMessage(data.response, 'ai');
            } else if (data.error) {
                this.addMessage('❌ ' + data.error, 'ai');
            }

        } catch (error) {
            if (loadingDiv) loadingDiv.remove();
            this.addMessage('❌ Lỗi kết nối. Vui lòng thử lại sau!', 'ai');
            console.error('Chat error:', error);
        } finally {
            // Re-enable input
            chatInput.disabled = false;
            sendButton.disabled = false;
            chatInput.focus();
        }
    }

    addMessage(content, type) {
        const chatMessages = document.getElementById('chatMessages');
        if (!chatMessages) return null;

        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${type}-message`;
        messageDiv.innerHTML = content;
        
        chatMessages.appendChild(messageDiv);
        chatMessages.scrollTop = chatMessages.scrollHeight;
        
        return messageDiv;
    }
}

// Initialize chatbot when page loads
document.addEventListener('DOMContentLoaded', function() {
    console.log('Chuyên gia Tài chính AI đã sẵn sàng!');
    
    // Initialize chatbot widget
    const chatbot = new ChatbotWidget();
    
    // Add some interactive effects
    const chatbotIcon = document.getElementById('chatbotIcon');
    if (chatbotIcon) {
        // Add hover effect
        chatbotIcon.addEventListener('mouseenter', function() {
            this.style.transform = 'scale(1.1)';
        });
        
        chatbotIcon.addEventListener('mouseleave', function() {
            this.style.transform = 'scale(1)';
        });
    }
});

// Export for external use
window.ChatbotWidget = ChatbotWidget;
