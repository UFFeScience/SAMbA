var app = new Vue({
    el: '#app',
    data: {
        details: {},
        task: {info: {}},
        showUsedData: false,
        selectedDataElementID: "",
        executionID: getUrlParameters("id", "", true),
        popupData: {
            hasFiles: false
        }
    },
    methods: {
        getDetails: function () {
            var context = this;
            var id = getUrlParameters("id", "", true);
            var taskID = getUrlParameters("taskID", "", true);
            $.ajax({
                url: '/api/execution/find/' + id,
                type: 'GET',
                success: function (result) {
                    context.details = result;
                }
            });

            $.ajax({
                url: '/api/task/info/' + id + "?taskID=" + taskID,
                type: 'GET',
                success: function (result) {
                    context.task = result;
                }
            });
            context.requestGraphData(id, taskID);
        },
        requestGraphData: function (id, taskID) {
            var context = this;
            var url = '/api/dataelement/graphOfTask/' + id + "?taskID=" + taskID;
            url += '&showUsedData=' + this.showUsedData;

            $.ajax({
                url: url,
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
        requestDataElementInfo: function (dataElementID) {
            var dataElement = this.chartData.nodes.get(dataElementID);
            var context = this;
            var url = "/api/dataelement/table/" + dataElementID + "?executionID=" + this.executionID;
            $("#contentTAB_ID").click();
            $("#dataElementModal").modal("show");

            console.log(dataElement);

            if (dataElement.taskID) {
                url += "&taskID=" + dataElement.taskID
            }

            $.ajax({
                url: url,
                type: 'GET',
                success: function (result) {
                    context.popupData.hasFiles = result.hasDataInRepository;
                    document.getElementById("tableWrapper").innerHTML = result.tableContent;
                    if (context.popupData.hasFiles) {
                        $("#fileTree")
                            .on('changed.jstree', function (e, data) {
                                if (data && data.action == "select_node") {
                                    var data = data.node.data;
                                    if (data) {
                                        context.downloadFile(data.executionID, data.filePath);
                                    }
                                }
                            }).jstree({
                            'core': {
                                'multiple': false,
                                'data': [
                                    result.filesTree
                                ]
                            }
                        })
                    }
                },
                error: function (error) {
                },
                complete: function () {

                }
            });
        },
        downloadFile: function (executionID, filePath) {
            window.open(encodeURI("/api/dataelement/download?executionID=" + executionID + "&filePath=" + filePath));
        },
        buildGraph: function (data) {
            var container = document.getElementById('graph');
            // var options = {
            //     // layout: {
            //     //     hierarchical: {sortMethod: 'hubsize'}
            //     // },
            //     physics: {stabilization: false},
            //     edges: {
            //         arrows: {to: true}
            //     }
            // };
            var nodes = new vis.DataSet(data["nodes"]);
            var edges = new vis.DataSet(data["edges"]);
            var _data = {
                nodes: nodes,
                edges: edges
            };
            var options = {
                // layout: {
                //     hierarchical: {
                //         direction: 'UD'
                //     }
                // },
                physics: {stabilization: false},
                edges: {
                    smooth: true,
                    arrows: {to: true}
                }
            };
            network = new vis.Network(container, _data, options);
            app.chartData = _data;
            network.on("doubleClick", function (params) {
                if (params.nodes.length > 0) {
                    var dataElementID = params.nodes[0];
                    app.selectedDataElementID = dataElementID;
                    app.requestDataElementInfo(dataElementID);
                }
            })
        }
    }
});


swal({
    title: 'Do you want to see what Data Collections were used to produce the output?',
    type: 'question',
    showCancelButton: true,
    confirmButtonColor: '#3085d6',
    cancelButtonColor: '#d33',
    confirmButtonText: 'Yes',
    cancelButtonText: 'No'
}).then(function (result) {
    if (result.value) {
        app.showUsedData = true;
    }
    app.getDetails();
});





