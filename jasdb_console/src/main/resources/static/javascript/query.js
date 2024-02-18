$(document).ready(function() {
    $(document).on("click", ".showDocument", function () {
        let json = $(this).data('content');
        let bag = $(this).attr("bag");
        let instance = $(this).attr("instance");

        let edBtn = $("#editBtn");
        edBtn.attr("bag", bag);
        edBtn.attr("instance", instance);
        edBtn.attr("json", JSON.stringify(json));
        $("#dataContent").JSONView(json);
    });

    loadInstances();

    $("#filterBtn").click(function(event) {
        event.preventDefault();

        let instanceList = $("#instanceSelector");
        let bagList = $("#bagSelector");
        let selectedInstance = instanceList.find('option:selected').val();
        let selectedBag = bagList.find('option:selected').val();

        doQuery(selectedInstance, selectedBag);
    });

    $(".editBtn").click(function(event) {
        event.preventDefault();

        $("#clsViewBtn").click();
        $("#editModal").modal('show');
        let edBtn = $("#editBtn");
        let jsonData = edBtn.attr('json');
        let bag = edBtn.attr("bag");
        let instance = edBtn.attr("instance");
        console.log("Starting to show edit dialog, data: " + jsonData + " on bag: " + bag + " on instance: " + instance);

        let saveBtn = $("#saveBtn");
        saveBtn.attr("bag", bag);
        saveBtn.attr("instance", instance);
        $("#dataEntity").val(jsonData);
    })

    $("#saveBtn").click(function(event) {
        event.preventDefault();

        let saveBtn = $("#saveBtn");
        let bag = saveBtn.attr("bag");
        let instance = saveBtn.attr("instance");
        let jsonData = $("#dataEntity").val();
        console.log("Updating Entity, data: " + jsonData + " on bag: " + bag + " on instance: " + instance);

        $.ajax({url: "/Instances(" + instance + ")/Bags(" + bag + ")/Entities", type: "PUT", data: jsonData, dataType: "json", contentType: "application/json; charset=utf-8", success: function() {
                console.log("Posted Updated Entity successfully");
                doQuery(instance, bag);

                $("#clsEditBtn").click();
        }}).fail(function(data) {
            $("#clsEditBtn").click();

            let statusCode = data.responseJSON.statusCode;
            let message = data.responseJSON.message;
            console.log("Failed to post entity, error code: " + statusCode + " and message: " + message);

            $("#errorMessage").html(message);
            $("#messageModal").modal('show');
        });
    });

    let root = $("#root");
    let preLoadInstance = root.attr('instance');
    let preLoadBag = root.attr('bag');
    if(preLoadInstance !== undefined && preLoadBag !==undefined) {
        doQuery(preLoadInstance, preLoadBag);
    }
});

function doQuery(instance, bag) {
    $("#docList").empty();

    let inputField = $("#inputField").val();
    let inputValue = $("#inputValue").val();

    let url = "/Instances(" + instance + ")/Bags(" + bag + ")/Entities"
    if(inputField !== null && inputField.length > 0 && inputValue !== null && inputValue.length > 0) {
        url += "(" + inputField + "=" + inputValue + ")";
    }
    $.get(url, function(data) {
        $.each(data.entities, function (i, entity) {
            let data = {
                "docId" : entity.__ID,
                "docJson" : JSON.stringify(entity),
                "instance" : instance,
                "bag" : bag
            }
            renderAndAppend("resultItem", data, "docList");
        })
    })
}

function loadInstances() {
    let instanceList = $("#instanceSelector");

    let root = $("#root");
    let preLoadInstance = root.attr('instance');
    let preLoadBag = root.attr('bag');

    $.get("/Instances", function(data) {
        $.each(data.instances, function(i, instance) {
            let instanceId = instance.instanceId;

            if(data.instances.length > 1) {
                let selected = false;
                if(instanceId === preLoadInstance) {
                    selected = true;
                }
                instanceList.append(new Option(instanceId, instanceId, selected, selected));
            } else {
                instanceList.append(new Option(instanceId, instanceId, true, true));
                loadBags(instanceId);
            }
        });
    })

    instanceList.change(function () {
        let selectedInstance = instanceList.find('option:selected').val();
        loadBags(selectedInstance);
    });
}

function loadBags(instance) {
    let bagList = $("#bagSelector");

    let root = $("#root");
    let preLoadBag = root.attr('bag');

    $.get("/Instances(" + instance + ")/Bags", function(data) {
        $.each(data.bags, function(i, bag) {
            let bagName = bag.name;
            if(data.bags.length > 1) {
                let selected = false;
                if(bagName === preLoadBag) {
                    selected = true;
                }

                bagList.append(new Option(bagName, bagName, selected, selected));
            } else {
                bagList.append(new Option(bagName, bagName, true, true));
            }

        });
    })
}