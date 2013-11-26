var fs = require('fs');
var Balancer = require('child-balancer');

var handlerScripts = [];

var config = {};
var workflow = {};
//= require('./router-config.global');

var routingTable = {};

function generateSender(gRoute, varName, varValue) {
    var handler = routingTable[gRoute];
    return function (message) {
        message.data[varName] = varValue;
        handler.send(message);
    }
}

function dispatchMessage(message) {
    var handler = routingTable[message.route];
    if (!handler) {
        //console.log("Dynamic resolving for",message.route);
        // lets try dynamic handler
        for (var r in routingTable) {
            // '>0', not '>=0' as we skip '$start' events
            if (routingTable.hasOwnProperty(r) && r.indexOf('$') > 0) {
                var vars = /\$(\w+)/.exec(r);
                var reg = new RegExp(r.replace('.', '\\.').replace(/\$(\w+)/, '(\\w+)'));
                var m = reg.exec(message.route);
                if (m) {
                    routingTable[message.route] = {
                        send: generateSender(r, vars[1], m[1])
                    };
                    handler = routingTable[message.route];
                    break;
                }
            }
        }
        if (!handler) {
            console.error('No handler for message ', message, Object.keys(routingTable));
            if (message.route[0]!='$')
                dispatchMessage({route:'error.noHandler',data:message});
        }
    }

    if (handler) handler.send(message);

}

function RouterStart() {
    //var handlers = {};
    handlerScripts.forEach(function (handlerFile) {
        var path = handlerFile;
        var mRoutes = require(path);
        var minLimit = 1;
        var maxLimit = 1;
        var concurrency = 1000;
        var toRegister = [];
        for (var mRoute in mRoutes) {
            if (!mRoutes.hasOwnProperty(mRoute) || mRoute[0] == '$') continue;
            var routeDef = config[mRoute];
            if (!routeDef) {
                // lets try to find extracting '*'
                for (var r in config) {
                    if (!config.hasOwnProperty(r)) continue;
                    var reg = new RegExp(r.replace('.', '\\.').replace('*', '\\w+'));
                    if (reg.test(mRoute)) routeDef = config[r];
                }
            }
            if (routeDef && routeDef.nodes && routeDef.nodes > minLimit) minLimit = routeDef.nodes;
            if (routeDef && routeDef.concurrency && routeDef.concurrency > minLimit) concurrency = routeDef.concurrency;
            if (routeDef && routeDef.maxNodes && routeDef.maxNodes > maxLimit && routeDef.maxNodes >= minLimit) maxLimit = routeDef.maxNodes;
            toRegister.push(mRoute);
        }

        var balancer = new Balancer('./router-worker', {
            min_limit: minLimit,
            max_limit: maxLimit,
            concurrency: concurrency,
            args: [path]
        });
        balancer.newWorkerHandler = function (worker) {
            worker.worker.send({
                cmd: '$workflow',
                workflow: workflow
            });
        };
        console.log("Configured ", path, ": min=", balancer.config.min_limit, " max=", balancer.config.max_limit, " conc=", balancer.config.concurrency);

        balancer.onMessage(dispatchMessage);

        toRegister.forEach(function (r) {
            routingTable[r] = balancer
        })
    });
}
function addHanldersDir(dir) {
    var handers = fs.readdirSync(dir);
    handers.forEach(function (file) {
        handlerScripts.push(dir + '/' + file);
    });
}

function ImportConfig(cfg) {
    for (var c in cfg) {
        if (!cfg.hasOwnProperty(c)) continue;
        config[c] = cfg[c];
    }
}
function ImportWorkflow(cfg) {
    for (var c in cfg) {
        if (!cfg.hasOwnProperty(c)) continue;
        workflow[c] = cfg[c];
    }
}
function RouterUse(m) {
    m(this);
}

module.exports = {
    config: ImportConfig,
    workflow: ImportWorkflow,
    start: RouterStart,
    addHandlersDir: addHanldersDir,
    use: RouterUse
};



