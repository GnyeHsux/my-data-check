function doCheckData() {
    layer.msg('数据加载中，稍等一下', {
        icon:16,
        shade:[0.1, '#fff'],
        time:false  //取消自动关闭
    })
    var tableList = $(".res_field_form").parseFormJSON();
    var source = $("#sourceForm").parseFormJSON()[0];
    var target = $("#targetForm").parseFormJSON()[0];

    var compareType = source.compareType;
    var postUrl = "/data/onlineCompare";
    if ("offline" === compareType) {
        postUrl = "/data/oflineCompare/source";
    }
    var postData = {
        "sourceDataSource": source,
        "targetDataSource": target,
        "compareLineList": tableList
    }
    $.ajax({
        url: postUrl,
        data: JSON.stringify(postData),
        type: "post",
        dataType: "json",
        contentType: "application/json; charset=utf-8",
        success: function (data) {
            layer.close(layer.index);
            if (data.code !== "0" && data.code !== undefined) {
                console.log(data)
                layer.msg(data.msg);
            } else {
                if ("offline" === compareType) {
                    // 输出 json 文件
                    console.log(data)
                    funDownload(JSON.stringify(data), "sourceData.json")
                } else {
                    // 展示比较结果
                    buildResultTable(data.data)
                }
            }
        },
        error: function (data) {
            layer.close(layer.index);
            layer.msg(data);
        }
    });
}


var buildResultTable = function (data) {
    var divHtml = "";
    console.log(data)
    data.forEach(function (element) {
            var compareRule = "相等";
            if (element.compareRule === "2") {
                compareRule = "不相等"
            }
            if (element.compareRule === "") {
                compareRule = "空值"
            }
            var tableHeadStr = "";
            var tableBodyStr = "";
            if (element.compareResultList.length > 0) {
                tableHeadStr = "<th>序号</th><th>数据来源</th>";
                var target = element.compareResultList[0].target;
                //生成表头
                var keyArray = [];
                for (fieldName in target) {
                    if (element.keyField.indexOf(fieldName) > -1 || element.compareFields.indexOf(fieldName) > -1) {
                        //则包含该元素
                        tableHeadStr = tableHeadStr + "<th class='pk-bgColor'>" + fieldName + "</th>";
                    } else {
                        tableHeadStr = tableHeadStr + "<th>" + fieldName + "</th>";
                    }
                    keyArray.push(fieldName)
                }
                // 生成 body
                for (i in element.compareResultList) {
                    var resultSource = element.compareResultList[i].source;
                    var idx = parseInt(i)+1;
                    var sourceStr = "<tr><td>" + idx +"</td><td>源库</td>"
                    for (fieldName of keyArray) {
                        if (element.keyField.indexOf(fieldName) > -1 || element.compareFields.indexOf(fieldName) > -1) {
                            //则包含该元素
                            sourceStr = sourceStr + "<td class='source-bgColor'>" + resultSource[fieldName] + "</td>";
                        } else {
                            sourceStr = sourceStr + "<td>" + resultSource[fieldName] + "</td>";
                        }
                    }
                    sourceStr = sourceStr + "</tr>";

                    var resultTarget = element.compareResultList[i].target;
                    var targetStr = "<tr><td>" + idx +"</td><td>目标库</td>"
                    for (fieldName of keyArray) {
                        if (element.keyField.indexOf(fieldName) > -1 || element.compareFields.indexOf(fieldName) > -1) {
                            //则包含该元素
                            targetStr = targetStr + "<td class='target-bgColor'>" + resultTarget[fieldName] + "</td>";
                        } else {
                            targetStr = targetStr + "<td>" + resultTarget[fieldName] + "</td>";
                        }
                    }
                    targetStr = targetStr + "</tr>";
                    tableBodyStr = tableBodyStr + sourceStr + targetStr;
                }

            }

            var str = "<div class=\"layui-colla-item\">\n" +
                "                <h2 class=\"layui-colla-title\">表：" + element.tableName + " 关联字段：" + element.keyField + " 比较字段：" + element.compareFields + " 比较类型：" + compareRule
                + " 结果条数：" + element.compareResultList.length + "</h2>\n" +
                "                <div class=\"layui-colla-content layui-show\">\n" +
                "                    <table class=\"layui-table\" lay-size=\"sm\">\n" +
                "                        <thead>\n" +
                "                        <tr>\n" + tableHeadStr +
                "                        </tr>\n" +
                "                        </thead>\n" +
                "                        <tbody>\n" + tableBodyStr +
                "                        </tbody>\n" +
                "                    </table>\n" +
                "                </div>\n" +
                "            </div>";

            divHtml = divHtml + str;
        }
    );

    $(".layui-collapse").html(divHtml)
}


var funDownload = function (content, filename) {
    // 创建隐藏的可下载链接
    var eleLink = document.createElement('a');
    eleLink.download = filename;
    eleLink.style.display = 'none';
    // 字符内容转变成blob地址
    var blob = new Blob([content]);
    eleLink.href = URL.createObjectURL(blob);
    // 触发点击
    document.body.appendChild(eleLink);
    eleLink.click();
    // 然后移除
    document.body.removeChild(eleLink);
}
//定义HTable对象
window.HTable = {
    trNum: 1,
    count: 0,
    DEL_TR: function (trNum) {
        $("#tr" + trNum).remove();
        this.trNum--;
        if (this.trNum == 0) {
            this.trNum = 1;
        }
        this.count--;
        if (this.count < 0) {
            this.count = 0;
        }
        $("#count").text(this.count);
    },
    ADD_TR: function (trNum) {
        var compareRule = "<select name='compareRule'><option value='1' selected>相等</option><option value='2'>不相等</option><option value='3'>空值</option></select>";
        var result = "<tr id='tr" + trNum + "'>" +
            "<td><div class='layui-iput-inline'><input type='text' name='tableName' class='layui-input' value='ftpaddr'/></div></td>" +
            "<td><div class='layui-iput-inline'><input type='text' placeholder='filedA' name='keyField' class='layui-input' value='ph_key'/></div></td>" +
            "<td><div class='layui-iput-inline'><input type='text' placeholder='filedA,filedB' name='compareFields' value='userid,passwd' class='layui-input'/></div></td>" +
            "<td><div class='layui-iput-inline'>" + compareRule + "</div></td>" +
            "<td><a class='layui-btn layui-btn-sm layui-btn-danger' onclick='HTable.DEL_TR(" + trNum + ")'>删除</a></td>" +
            "</tr>";
        this.trNum++;
        this.count++;
        $("#count").text(this.count);
        return result;
    },
    TR_ROW: function (trNum, trHtml) {

    }

}

//原生JS的方式实现构建JSON数组
$.fn.parseFormJSON = function () {
    var result = [];
    var json = {};
    var data = this.serializeArray();
    if (data.length == 0) {
        return [];
    } else {
        for (var i = 0; i < data.length; i++) {
            var key = data[i].name;
            var value = data[i].value;
            if (json.hasOwnProperty(key)) {//当存在是
                result.push(json);
                //var newJson = {};
                json = {};//
                json[key] = value;
            } else {
                json[key] = value;
                if (i == data.length - 1) {
                    result.push(json);
                }
            }
        }
    }
    return result;
};