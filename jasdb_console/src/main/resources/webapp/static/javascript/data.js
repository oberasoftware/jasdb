$(document).ready(function() {
    function toggleChevron(e) {
        $(e.target)
            .prev('.panel-heading')
            .find('i.indicator')
            .toggleClass('glyphicon-chevron-down glyphicon-chevron-right');
    }
    $('#accordion').on('hidden.bs.collapse', toggleChevron);
    $('#accordion').on('shown.bs.collapse', toggleChevron);

    $(document).on("click", ".addDocument", function () {
        var bagName = $(this).data('id');
        $(".modal-body #bag").val( bagName );
    });

    $(document).on("click", ".showDocument", function () {
        var json = $(this).data('content');

        $(".modal-body #dataContent").JSONView(json);
    });
});