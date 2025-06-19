document.addEventListener('DOMContentLoaded', function() {
    // Get all menu items
    const menuItems = document.querySelectorAll('.menu-item');
    const iframes = document.querySelectorAll('.BI');

    // Add click event listener to each menu item
    menuItems.forEach(item => {
        item.addEventListener('click', function(e) {
            e.preventDefault();
            
            // Remove active class from all menu items
            menuItems.forEach(menuItem => menuItem.classList.remove('active'));
            // Add active class to clicked menu item
            this.classList.add('active');

            // Get the clicked menu item's text
            const menuText = this.querySelector('span').textContent.toLowerCase();

            // Hide all iframes
            iframes.forEach(iframe => iframe.classList.remove('active'));

            // Show the corresponding iframe
            switch(menuText) {
                case 'index':
                    document.querySelector('.index-iframe').classList.add('active');
                    break;
                case 'stock':
                    document.querySelector('.stock-iframe').classList.add('active');
                    break;
                case 'company':
                    document.querySelector('.company-iframe').classList.add('active');
                    break;
            }
        });
    });
}); 
