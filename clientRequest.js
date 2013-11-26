process.on('message', function(data) {
    console.log("client reply");
    process.send({id:data.id, msg:"Hello"});
})