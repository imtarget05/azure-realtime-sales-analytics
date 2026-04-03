document.addEventListener("DOMContentLoaded", function () {
    var el = document.getElementById("confidence-bar");
    if (el) {
        var lower = parseFloat(el.dataset.lower) || 0;
        var upper = parseFloat(el.dataset.upper) || 1;
        var predicted = parseFloat(el.dataset.predicted) || 0;
        var pct = (upper !== lower) ? ((predicted - lower) / (upper - lower) * 100) : 50;
        el.style.width = pct + "%";
    }
});
