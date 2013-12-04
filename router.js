var fs = require('fs');
var Balancer = require('child-balancer');

var handlerScripts = [];

var config = {};
var workflow = {};
//= require('./router-config.global');

var routingTable = {};

var balancers = [];

function sendToHandler(handler,message) {
    if (handler.config && handler.config.concurrency && handler.config.concurrency>0)
        handler.send({'$complete':message});
    else
        handler.send(message);
}

function sendAll(handlers, message) {
    for(var i =0;i<handlers.length;i++)
        sendToHandler(handlers[i],message);
}

function generateSender(gRoute, varName, varValue) {
    var handler = routingTable[gRoute];
    if (handler.push)
        return function (message) {
            message.data[varName] = varValue;
            sendAll(handler,message);
        }
    return function (message) {
        message.data[varName] = varValue;
        sendToHandler(handler,message);
    }
}

function dispatchMessage(message) {
    var handler = routingTable[message.route];
    if (!handler && message.route[0]!='$') {
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
    }
    if (!handler) {
        if (message.route[0]!='$' && message.route !='error.noHandler') {
            console.error('No handler for message ', message, Object.keys(routingTable));
            dispatchMessage({route:'error.noHandler',data:message});
        }
    }
    if (handler) {
        if (handler.push)
            sendAll(handler,message);
        else
            sendToHandler(handler,message);
    }

}
function registerSender(route, sender){
    if (routingTable[route]) {
        if (routingTable[route].push)
            routingTable[route].push(sender);
        else
            routingTable[route] = [routingTable[route],sender];
    } else
        routingTable[route] = sender;
}

function RouterStart() {
    //var handlers = {};
    handlerScripts.forEach(function (handlerFile) {
        var path = handlerFile;
        var mRoutes = require(path);

        var balancerConfig = {
            min_limit: 1,
            max_limit: 1,
            concurrency: 0,
            args: [path]
        };
        if (mRoutes['$config']) {
            var config = mRoutes['$config'];
            for (var k in config) {
                if (config.hasOwnProperty(k)) balancerConfig[k] = config[k];
            }
        }

        var balancer = new Balancer(__dirname+'/router-worker', balancerConfig);
        balancer.newWorkerHandler = function (worker) {
            worker.worker.send({
                cmd: '$workflow',
                workflow: workflow
            });
            send('$worker.created',worker);
        };
        balancers.push(balancer);
        console.log("Configured ", path, ": min=", balancer.config.min_limit, " max=", balancer.config.max_limit, " conc=", balancer.config.concurrency);

        balancer.onMessage(dispatchMessage);

        for (var mRoute in mRoutes) {
            if (!mRoutes.hasOwnProperty(mRoute) || mRoute[0] == '$') continue;
            registerSender(mRoute,balancer);
        }
    });
}

function addHanldersDir(dir) {
    var handers = fs.readdirSync(dir);
    handers.forEach(function (file) {
        handlerScripts.push(dir + '/' + file);
    });
}

function addHanlder(file) {
    handlerScripts.push(file);
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


function send(route, message) {
    var envelope = {route: route, data: message};
    if (process.send) {
        if (!process.connected)
            console.warn("Reply from disconnected worker discarded:",message);
        else
            process.send(envelope);
    }
    else
        dispatchMessage(envelope);
}
function complete(route, message) {
    var envelope = {'$complete':{route: route, data: message}}
    if (process.send)
        process.send(envelope);
    else
        dispatchMessage(envelope);
}

function stop() {
    balancers.forEach(function(b){
        b.disconnect();
    })
}

function addListener(message,callBack) {
    registerSender(message,{
        send:callBack
    });
}

module.exports = {
    config: ImportConfig,
    workflow: ImportWorkflow,
    start: RouterStart,
    addHandlersDir: addHanldersDir,
    addHandler: addHanlder,
    use: RouterUse,
    send: send,
    complete: complete,
    stop: stop,
    on:addListener

};



