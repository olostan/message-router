var script = process.argv[2];
console.log("[Worker] Starting", script);
var handler = require(script);

//var workflowConfig = require('./workflow.json');
var workflowConfig;

var router = {
    send: function (route, message) {
        process.send({route: route, data: message});
    }
};
var noop = function () {
};
(handler['$start'] || noop)(router);

var _nextCash = {};
function getNext(route) {
    var getter = function (msg, param) {
        var cache = _nextCash[route];
        if (!cache) throw Error("No workflow step for route '"+route+"'");
        if (cache.generator) {
            if (!workflowConfig) throw "Worker can't find workflow";
            var next = workflowConfig[route];
            if (!next) {
                for (var r in workflowConfig) {
                    var reg = new RegExp(r.replace('.', '\\.').replace('*', '\\w+'));
                    if (reg.test(route))
                        next = workflowConfig[r];
                }
            }
            //if (!next)
            //    console.warn("Can't find next workflow step for ", route, "msg:",msg);
            if (next && next.indexOf('$') > 0) {
                var s = "process.send({route:";
                s += ('"' + next.replace(/\$(\w+)/g, '"+msg.$1+"') + '"').replace('+""', '');
                s += ",data:msg})";
                //console.log("generated function(msg) {",s,"}");
                cache = Function('msg', s);

            } else if (next)
                cache = function (msg) {
                    process.send({route: next, data: msg});
                }
            else cache = null;
            _nextCash[route] = cache;
        }
        if (cache)  cache(msg, param);
    }
    getter.generator = true;
    return getter;
}
for (var k in handler) {
    if (k[0] != '$' && handler.hasOwnProperty(k)) {
        _nextCash[k] = getNext(k);

    }
}

process.on('message', function (messageEnvelope) {
    if (messageEnvelope.cmd == '$workflow') {
        workflowConfig = messageEnvelope.workflow;
        return;
    }
    if (messageEnvelope.cmd == '$data' ) {
        if (handler.setData) handler.setData(messageEnvelope.data);
        return;
    }

    var handlerFn = handler[messageEnvelope.route];
    if (!handlerFn) {
        for (var r in handler) {
            if (r.indexOf('$') > 0) {
                var regExp = new RegExp(r.replace('.', '\\.').replace(/\$(\w+)/, '(\\w+)'));
                if (regExp.test(messageEnvelope.route)) {
                    handlerFn = handler[k];
                    handler[messageEnvelope.route] = handlerFn;
                    _nextCash[messageEnvelope.route] = getNext(messageEnvelope.route);
                    break;
                }
            }
        }
    }
    if (handlerFn)
        handlerFn(messageEnvelope.data, _nextCash[messageEnvelope.route]);
    else
        console.warn('[Worker] Can\'t find handler for message:', messageEnvelope);
});
