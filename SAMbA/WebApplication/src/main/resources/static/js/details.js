var app = new Vue({
    el: '#app',
    data: {
        details: {}
    },
    methods: {
        getDetails: function () {
            var context = this;
            var id = getUrlParameters("id", "", true);
            $.ajax({
                url: '/api/execution/find/' + id,
                type: 'GET',
                success: function (result) {
                    context.details = result;
                    context.requestGraphData(id);
                },
                error: function (error) {
                },
                complete: function () {
                }
            });
        },
        requestGraphData: function (id) {
            var context = this;
            $.ajax({
                url: '/api/task/graph/' + id,
                type: 'GET',
                success: function (result) {
                    context.buildGraph(result);
                },
                error: function (error) {
                },
                complete: function () {
                }
            });
        },
        buildGraph: function (data) {
            var context = this;
            var container = document.getElementById('graph');
            var options = {
                layout: {
                    hierarchical: {
                        sortMethod: 'directed'
                    }
                },
                edges: {
                    smooth: true,
                    arrows: {to: true}
                }
            };
            network = new vis.Network(container, data, options);
            network.on("doubleClick", function (params) {
                if (params.nodes.length > 0) {
                    context.showAllDataElementsOf(params.nodes[0]);
                }
            })
        },
        showAllDataElementsOf: function (taskId) {
            var id = getUrlParameters("id", "", true);
            window.location.href = "/execution/taskgraph?id=" + id + "&taskID=" + taskId;
        }
    }
});

app.getDetails();


if (document.getElementById('_hasFiles').value == "true") {
    $.ajax({
        url: "/api/dataelement/repositoryFileTree/" + getUrlParameters("id", "", true),
        type: 'GET',
        success: function (result) {
            $("#fullRepositoryFileTree")
                .on('changed.jstree', function (e, data) {
                    if (data && data.action == "select_node") {
                        var data = data.node.data;
                        if (data) {
                            window.open(encodeURI("/api/dataelement/download?executionID=" + data.executionID + "&filePath=" + data.filePath));
                        }
                    }
                }).jstree({
                'core': {
                    'multiple': false,
                    'data': [
                        result
                    ]
                }
            })
        },
        error: function (error) {
        },
        complete: function () {

        }
    });

}

