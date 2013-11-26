
function doTest(fn,name) {
    var c = 0;
    var start = (new Date()).getTime();
    var end;
    do {
        fn();
        c++;
        end = (new Date()).getTime();;
    } while (end-start < 1000)
    console.log(name,":",c);
}

var sourceArr = [];
for (var i = 0;i<10000;i++) {
    sourceArr.push(Math.random());
}
var t = 1000;
function shifting() {
    var arr = sourceArr.slice();
    for (var i=0;i<t;i++)
        arr.shift();
}
function splicing() {
    var arr = sourceArr.slice();
    for (var i=0;i<t;i++)
        arr.splice(2,1);
}
function LinkedArray() {
    this.root = null;
}
LinkedArray.prototype.add = function(val) {
    var node = { val: val, next: null };
    if (!this.root) {
        this.root = node;
    } else
        this.root.next = node;
}
LinkedList.prototype.get = function(n) {
    console.log()
}
LinkedArray.prototype.getAndRemove = function(n) {
    var prev = this.root;
    while (n-->1) prev = prev.next;
    var current = prev.next;
    prev.next = current.next;
    return current.val;

}
LinkedArray.prototype.toArray = function() {
    var r = [];
    var n = this.root;
    while (n!=null) {
        r.push(n.val);
        n = n.next;
    }
    return r;
}

var linked = new LinkedArray();
linked.add("a");
linked.add("b");
linked.add("c");
console.log(linked.getAndRemove(1));
console.log(linked.toArray());



/*console.log("start.");
doTest(shifting,'shifting');
doTest(splicing,'splicing');
*/
