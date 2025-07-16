// Toggle tooltip visibility on click
document.addEventListener('DOMContentLoaded', function() {
    const validationCells = document.querySelectorAll('.validation-cell');
    
    validationCells.forEach(cell => {
        cell.addEventListener('click', function(e) {
            e.preventDefault();
            
            // Toggle clicked state
            this.classList.toggle('clicked');
            
            // Hide other open tooltips
            validationCells.forEach(otherCell => {
                if (otherCell !== this) {
                    otherCell.classList.remove('clicked');
                }
            });
        });
    });
    
    // Close tooltip when clicking outside
    document.addEventListener('click', function(e) {
        if (!e.target.closest('.validation-cell')) {
            validationCells.forEach(cell => {
                cell.classList.remove('clicked');
            });
        }
    });
});