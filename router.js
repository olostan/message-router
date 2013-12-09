"use strict";

var fs = require('fs');
var Balancer = require('child-balancer');

const debugRouting = false;
const debugTiming = true;

var handlerScripts = [];

var config = {};
var workflow = {};
//= require('./router-config.global');

var routingTable = {};

var balancers = [];

var timing = {};
function getTiming(route) {
    var t = timing[route];
    if (!t) {
        t = {
            serialization:0,
            count:0
        };
        timing[route]=t;
    }
    return t;
}

function generateTimedWrapper(sender) {
    var oldSend = sender.send;
    sender.send = function timingSendWrapper(msg) {
        msg.$timing = new Date();
        oldSend.call(sender,msg);
    }
}

function sendToHandler(handler,message) {
    if (debugRouting) console.log("[RT] --> ",message.route);

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
        };
    return function (message) {
        message.data[varName] = varValue;
        sendToHandler(handler,message);
    };
}

function dispatchMessage(message) {
    var t;
    if (debugTiming && message.$timing) {
        var end = new Date();
        t = getTiming(message.route);
        t.serialization +=  end-new Date(message.$timing);
        t.count ++;
    }

    if (message.cmd == '$timing') {
        t = getTiming(message.route);
        t.serialization += message.timing|0;
        t.count ++;
        return;
    }

    var handler = routingTable[message.route];
    if (!handler && message.route[0]!=='$') {
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
        if (message.route[0]!=='$' && message.route !=='error.noHandler') {
            console.error('No handler for route', message.route);
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

function send(route, message) {
    var envelope = {route: route, data: message};
    if (process.send) {
        if (debugTiming)
            envelope.$timing = new Date();

        if (debugRouting) console.log("[RT] <-- ",route);
        if (!process.connected)
            console.warn("Reply from disconnected worker discarded:",message);
        else
            process.send(envelope);
    }
    else
        dispatchMessage(envelope);
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
        var mRoutes = require(handlerFile);

        var balancerConfig = {
            min_limit: 1,
            max_limit: 1,
            concurrency: 0,
            pulseTime: 5000,
            args: [handlerFile]
        };
        if (mRoutes.$config) {
            var config = mRoutes.$config;
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
            if (debugTiming) generateTimedWrapper(worker);
            send('$worker.created',worker);
        };



        balancers.push(balancer);
        console.log("Configured ", handlerFile, ": min=", balancer.config.min_limit, " max=", balancer.config.max_limit, " c=", balancer.config.concurrency);

        balancer.onMessage(dispatchMessage);

        for (var mRoute in mRoutes)
            if (mRoutes.hasOwnProperty(mRoute) && mRoute[0] !== '$') {
                registerSender(mRoute, balancer);
            }

    });
}

function addHandlersDir(dir) {
    var handlers = fs.readdirSync(dir);
    handlers.forEach(function (file) {
        handlerScripts.push(dir + '/' + file);
    });
}

function addHandler(file) {
    handlerScripts.push(file);
}


function ImportConfig(cfg) {
    for (var c in cfg)
        if (cfg.hasOwnProperty(c)) config[c] = cfg[c];

}
function ImportWorkflow(cfg) {
    for (var c in cfg)
        if (cfg.hasOwnProperty(c)) workflow[c] = cfg[c];

}
function RouterUse(m) {
    m(this);
}


function complete(route, message) {
    var envelope = {'$complete':{route: route, data: message}};
    if (process.send)
        process.send(envelope);
    else
        dispatchMessage(envelope);
}

function stop() {
    balancers.forEach(function(b){
        b.disconnect();
    });
}

function addListener(message,callBack) {
    var callBackWrapper = function(msg) {
        callBack(msg.data);
    };
    registerSender(message,{
        send:callBackWrapper
    });
}

module.exports = {
    config: ImportConfig,
    workflow: ImportWorkflow,
    start: RouterStart,
    addHandlersDir: addHandlersDir,
    addHandler: addHandler,
    use: RouterUse,
    send: send,
    complete: complete,
    stop: stop,
    on:addListener,
    timing:timing

};



