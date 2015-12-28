var MongoOplog = Npm.require('mongo-oplog');


OpLogEvents = function (uri, filter, commandMgr, docUtil) {
    this.uri = uri;
    this.filter = filter;
    this.commandManager = commandMgr;
    this.docUtil = docUtil;
};

OpLogEvents.prototype.run = function () {
    try {
        var self = this;

        var oplog = MongoOplog(self.uri, {ns: self.filter}).tail();

        /*
        oplog.on('op', function (data) {
         console.log(data);
        });
         */
        oplog.on('insert', function (doc) {
            //console.log(doc.op);
            console.log(doc.op);
            self.ins(doc);
        });

        oplog.on('update', function (doc) {
            console.log(doc);
            console.log(doc.op);
            self.upd(doc);

        });

        oplog.on('delete', function (doc) {
            console.log(doc.op);
            self.del(doc);
        });

        oplog.on('error', function (error) {
            console.log(error);
            //throw error;
        });

        oplog.on('end', function () {
            console.log('Stream ended');
        });

        oplog.stop(function () {
            console.log('server stopped');
        });

        oplog.on('tail', function () {
            console.log('tail');
        });
    }
    catch (error) {
        console.log("OpLogEvents creation failed");
        throw error;

    }
};


OpLogEvents.prototype.ins = function (doc) {
};

OpLogEvents.prototype.upd = function (doc) {
};

OpLogEvents.prototype.del = function (doc) {
};

OpLogEvents.prototype.getCollectionName = function(doc) {
    return doc.ns.split('.')[1];
};


OpLogWrite = function (uri, filter, commandMgr, docUtil) {
    OpLogEvents.call(this, uri, filter, commandMgr, docUtil);

};


OpLogWrite.prototype = Object.create(OpLogEvents.prototype);


OpLogWrite.prototype.ins = function (doc) {
    //var future = new Future();
    console.log('insert ' + doc.o__id.toString());
    var sql = this.commandManager.prepareInsert(this.getCollectionName(doc), doc);
    console.log(sql);
};//.future();

OpLogWrite.prototype.upd = function (doc) {
    console.log('update '+doc.o2._id.toString());
    var sql = this.commandManager.prepareUpdate(this.getCollectionName(doc), doc);

    sql = this.docUtil.renameLinkFields(this.getCollectionName(doc), sql);
    console.log(sql);
};

OpLogWrite.prototype.del = function (doc) {
    console.log('delete '+doc.o._id.toString()());
    var sql = this.commandManager.prepareDelete(this.getCollectionName(doc), doc);
    console.log(sql);

};



