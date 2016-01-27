var MongoOplog = Npm.require('mongo-oplog');
var Future = Npm.require('fibers/future');


/*
 OpLogEvents
 */

OpLogEvents = function (uri, filter) {
    this.uri = uri;
    this.filter = filter;

};


OpLogEvents.prototype.start = function () {
    try {
        var self = this;

        var oplog = MongoOplog(self.uri, {ns: self.filter}).tail();

        /*
        oplog.on('op', function (data) {
         console.log(data);
        });
         */
        oplog.on('insert', function (doc) {

            self.writeRecord(doc, 'i');//.detach();
        });//.future());

        oplog.on('update', function (doc) {
            self.writeRecord(doc, 'u');
        });

        oplog.on('delete', function (doc) {
            self.writeRecord(doc, 'd');//.detach();
        });//.future());

        oplog.on('error', function (error) {
            console.log(error);
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
OpLogWrite = function (uri, filter, connection, dbTables) {
    OpLogEvents.call(this, uri, filter);
    this.dbTables = dbTables;
    this.connection = connection;
    this.counters = {ins: 0, upd: 0, del: 0, err: 0};

    this.syncdblog = new Mongo.Collection('syncdb_log');
    //this.cmdMgr = new OpSequelizeCommandManager(connection, dbTables);

}

OpLogWrite.prototype = Object.create(OpLogEvents.prototype);


OpLogWrite.prototype.writeRecord = function (doc, op) {
    try {
        var self = this;
        var future = new Future();

        var ret = false;

        //var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = '';

        var tableName = self.getCollectionName(doc);

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);


        cmdMgr.ee.on('beforeExecSql', function (tableName, sql, doc, action) {
            self.writeDbLog(tableName, sql, doc, action);
        });

        sql = cmdMgr.prepareSql(tableName, doc, op).wait();
        future.return(sql != '' ? cmdMgr.execSql(sql, tableName, doc, op).wait() : true);


        return future.wait();


    }
    catch (e) {
        console.log(e);
        future.return(false);
        return future.wait();
    }
}.future();

OpLogWrite.prototype.writeDbLog = function (tableName, sql, doc, action) {


    var self = this;

    //var future = new Future;

    self.syncdblog.insert({
        coll: tableName,
        oper: action,
        data: doc,
        command: sql,
        created: {ts: new Date(), app: "integrator"}
    });

    //future.return(true);


    //return future.wait();
}//.future();