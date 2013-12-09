var script = process.argv[2];
/**
 * @dict
 * @type {*}
 */
var handler = require(script);

console.log("[Worker] Starting", script, "["+process.pid+"]");

//var workflowConfig = require('./workflow.json');


/***
 * @dict
 */
var workflowConfig;

var router = {
    send: function (route, message) {
        process.send({route: route, data: message});
    }
};

var noOperation = function () {};

(handler['$start'] || noOperation)(router);


var _nextCash = {};
function getNext(route) {
    var getter = function (msg, param) {
        var completing = false;
        //console.log("generating getter for",route);
        var cache = _nextCash[route];
        if (!cache) throw Error("No workflow step for route '" + route + "'");
        if (cache.generator) {
            if (!workflowConfig) throw "Worker can't find workflow";
            var next = workflowConfig[route];
            if (!next) {
                next = workflowConfig["@"+route];
                completing = true;
            }
            if (!next)
            {

                for (var r in workflowConfig) {
                    if (!workflowConfig.hasOwnProperty(r)) continue;
                    var reg = new RegExp(r.replace('.', '\\.').replace('*', '\\w+'));
                    if (reg.test(route))
                        next = workflowConfig[r];
                    else if (reg.test('@'+route)) {
                        next = workflowConfig[r];
                        completing = true;
                    }
                }
            }
            //if (!next)  console.warn("Can't find next workflow step for ", route, "msg:",msg);
            if (next) {
                //console.log("Completing ",route,"->",next,"?", completing);
                if (next.indexOf('$') > 0) {
                    // process workflow events:
                    //    log.$type => log.{msg.type}

                    var message = "{route:";
                    message += ('"' + next.replace(/\$(\w+)/g, '"+msg.$1+"') + '"').replace('+""', '');
                    message += ",data:msg}";
                    if (completing)
                        message = '{"$complete":'+message+"}";

                    cache = Function('msg', 'process.send('+message+')');

                } else
                cache =  completing?
                    function (msg) { process.send({'$complete':{route: next, data: msg}}); }
                    :
                    function (msg) { process.send({route: next, data: msg}); }

            }
            else cache = noOperation;
            _nextCash[route] = cache;
        }
        if (cache)  cache(msg, param);
    };
    getter.generator = true;
    return getter;
}

(function () {
    for (var k in handler) {
        if (k[0] != '$' && handler.hasOwnProperty(k)) {
            _nextCash[k] = getNext(k);
        }
    }
})();

process.on('message', function (messageEnvelope) {
    if (messageEnvelope.cmd == '$workflow') {
        workflowConfig = messageEnvelope.workflow;
        return;
    }
    if (messageEnvelope.cmd == '$data') {
        if (handler.setData) { //noinspection JSCheckFunctionSignatures
            handler.setData(messageEnvelope.data);
        }
        return;
    }

    var handlerFn = handler[messageEnvelope.route];
    if (!handlerFn) {
        for (var r in handler) {
            if (handler.hasOwnProperty(r) && r.indexOf('$') > 0) {
                var regExp = new RegExp(r.replace('.', '\\.').replace(/\$(\w+)/, '(\\w+)'));
                if (regExp.test(messageEnvelope.route)) {
//                    console.log("Test passed for ", messageEnvelope.route, " passed for",r);
                    handlerFn = handler[r];
                    handler[messageEnvelope.route] = handlerFn;
                    _nextCash[messageEnvelope.route] = getNext(messageEnvelope.route);
                    break;
                }
            }
        }
    }
    if (typeof(handlerFn) == 'object') {
        console.error("Handler for route", messageEnvelope.route, " shouldn't be an object:", handlerFn);
    }
    if (handlerFn)
        handlerFn(messageEnvelope.data, _nextCash[messageEnvelope.route]);
    else
        console.warn('[Worker] Can\'t find handler for message:', messageEnvelope);
});

function messageDiscarded(msg) {
    console.warn("Message from disconnected worker discarded:",msg);
}
var heapdump;
try {
 heapdump = require('heapdump');
} catch(e) {}

process.on('disconnect',function(){
    console.log('[Worker] Finished ',script,'['+process.pid+']');
    for(var k in _nextCash)
        if (_nextCash.hasOwnProperty(k)) _nextCash[k] = messageDiscarded;
    if (heapdump) {
        var p = script.split('/');
        var file = p[p.length-1];
//        console.log("Writing dumps");
//        heapdump.writeSnapshot('logs/dump-'+Date.now()+'-'+file+'-'+process.pid+'.heapsnapshot');
    }
});
