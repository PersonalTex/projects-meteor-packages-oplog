var MongoOplog = Npm.require('mongo-oplog');
var Future = Npm.require('fibers/future');


/*
 OpLogEvents
 */

OpLogEvents = function (uri, filter) {
    this.uri = uri;
    this.filter = filter;
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
            console.log(doc.op);
            self.ins(doc);

        })

        oplog.on('update', function (doc) {
            console.log(doc);
            console.log(doc.op);
            //self.upd(doc);

        });

        oplog.on('delete', function (doc) {
            console.log(doc.op);
            //self.del(doc);
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



OpLogEvents.prototype.getCollectionName = function(doc) {
    return doc.ns.split('.')[1];
};


/*
 OpLogWrite
 */
OpLogWrite = function (uri, filter, connection, dbUtil) {
    OpLogEvents.call(this, uri, filter);
    this.dbUtil = dbUtil;
    this.connection = connection;
};

OpLogWrite.prototype = Object.create(OpLogEvents.prototype);



OpLogWrite.prototype.ins = function (doc) {
    try {

        self = this;

        //future = new Future();

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbUtil);

        cmdMgr.prepareInsert(this.getCollectionName(doc), doc);
        //sql = this.options.dbUtil.renameLinkFields(this.getCollectionName(doc), sql);

        console.log(cmdMgr.sql);
        var ret = cmdMgr.execSql(doc, 'i').wait();
        console.log(ret == true ? "insert success " + doc.o._id.toString() : "insert fail " + doc.o._id.toString());
        //future.return(ret);

        //return future.wait();
    }
    catch (e) {
        console.log(e);

    }

}.future();

/*
OpLogWrite.prototype.upd = function (doc) {
 try {
 var sql = this.options.commandManager.prepareUpdate(this.getCollectionName(doc), doc);
 //sql = this.docUtil.renameLinkFields(this.getCollectionName(doc), sql);
 console.log(sql);

 ret = this.options.commandManager.execSql(sql, doc, 'u').wait();
 console.log(ret == true ? "updated success " + doc.o2._id.toString() : "update fail " + doc.o2._id.toString());
 }
 catch(e) {
 console.log (e);

 }
}.future();
 */
/*
OpLogWrite.prototype.del = function (doc) {
 try {
 var sql = this.options.commandManager.prepareDelete(this.getCollectionName(doc), doc);
 console.log(sql);
 ret = this.options.commandManager.execSql(sql, doc, 'd').wait();
 console.log(ret == true ? "delete success " + doc.o._id.toString() : "delete fail " + doc.o._id.toString());
 }
 catch(e) {
 console.log (e);
 }
 }.future();
 */


