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
        let bagName = $(this).attr('bag');
        $(".modal-body #bag").val( bagName );
    });

    $("#addEntityBtn").click(function(event) {
        event.preventDefault();

        let bag = $("#bag").val();
        let json = $("#dataEntity").val();
        let instance = $("#instanceRoot").attr("instance");

        $.ajax({url: "/Instances(" + instance + ")/Bags(" + bag + ")/Entities", type: "POST", data: json, dataType: "json", contentType: "application/json; charset=utf-8", success: function() {
            console.log("Posted Entity successfully, reloading page");

            window.location.href = "/data/" + instance;
        }}).fail(function(data) {
            $("#clsBtn").click();

            let statusCode = data.responseJSON.statusCode;
            let message = data.responseJSON.message;
            console.log("Failed to post entity, error code: " + statusCode + " and message: " + message);

            $("#errorMessage").html(message);
            $("#messageModal").modal('show');
        });
    })

    $(document).on("click", ".showDocument", function () {
        let json = $(this).data('content');

        $(".modal-body #dataContent").JSONView(json);
    });
});