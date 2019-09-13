var app = new Vue({
    el: '#app',
    data: {
        details: {},
        showCluster: false,
        nodeLabelType: "ID",
        executionID: null,
        selectedDataElementID: "",
        popupData: {
            hasFiles: false
        }
    },
    methods: {
        getDetails: function () {
            var context = this;
            var id = getUrlParameters("id", "", true);
            this.executionID = id;
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
        requestGraphData: function (id, nodeLabelType) {
            var context = this;
            var url = '/api/dataelement/graph/' + id;
            url += '?showCluster=' + this.showCluster;
            if (nodeLabelType) {
                url += "&nodeLabelType=" + nodeLabelType
            }

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
            var options = {
                layout: {
                    randomSeed: 2
                    // hierarchical: {sortMethod: 'hubsize'}
                },
                physics: {stabilization: false},
                edges: {
                    arrows: {to: true}
                }
            };


            var x = -container.clientWidth / 2 + 50;
            var y = -container.clientHeight / 2 + 50;
            var step = 40;

            data["nodes"].push({
                id: 1000,
                x: x,
                y: y,
                label: 'Input Data',
                value: 1,
                shadow: true,
                "shape": "box",
                fixed: true,
                physics: false,
                color: "#FFF"
            });
            var legenda = data.legenda;

            for (var i = 0; i < legenda.length; i++) {
                var node = legenda[i];
                node.id = 1001 + i;
                node.x = x;
                node.y = y + ((i + 1) * step);
                data["nodes"].push(node)
            }

            var nodes = new vis.DataSet(data["nodes"]);
            var edges = new vis.DataSet(data["edges"]);
            var _data = {
                nodes: nodes,
                edges: edges
            };
            app.chartData = _data;
            var network = new vis.Network(container, _data, options);
            network.on("doubleClick", function (params) {
                if (params.nodes.length > 0) {
                    var dataElementID = params.nodes[0];
                    app.selectedDataElementID = dataElementID;
                    app.requestDataElementInfo(dataElementID);
                }
            })
        },
        onSelectNodeLabelType: function (labelName) {
            if (labelName === this.nodeLabelType) {
                return;
            }
            this.nodeLabelType = labelName;
            this.requestGraphData(this.executionID, labelName);
        }
    }
});

$("#selectNodeLabelGroup :input").closest('label').click(function () {
    var inputElement = this.getElementsByTagName('input')[0];
    var labelName = inputElement.value;
    app.onSelectNodeLabelType(labelName);
});


swal({
    title: 'Do you want to see what Data Collections don\'t pass in the Filter Transformations?',
    type: 'question',
    showCancelButton: true,
    confirmButtonColor: '#3085d6',
    cancelButtonColor: '#d33',
    confirmButtonText: 'Yes',
    cancelButtonText: 'No'
}).then(function (result) {
    if (result.value) {
        app.showCluster = true;
    }
    app.getDetails();
});





