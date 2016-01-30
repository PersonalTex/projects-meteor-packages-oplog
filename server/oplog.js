var MongoOplog = Npm.require('mongo-oplog');
var Future = Npm.require('fibers/future');



/*
 OpLogWrite
 */
OpLogWrite = function (uri, filter, connection, dbTables) {
    this.uri = uri;
    this.filter = filter;
    this.dbTables = dbTables;
    this.connection = connection;
    timerObserveSyncDbLog = -1;

    this.syncdblog = new Mongo.Collection('syncdb_log');
    //this.cmdMgr = new OpSequelizeCommandManager(connection, dbTables);
}

OpLogWrite.prototype.start = function () {
    try {
        var self = this;

        this.observeSyncDbLog(true);

        var oplog = MongoOplog(self.uri, {ns: self.filter}).tail();

        /*
        oplog.on('op', function (data) {
         console.log(data);
        });
         */
        oplog.on('insert', function (doc) {

            self.writeRecord(doc, 'i');
        });

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


OpLogWrite.prototype.getCollectionName = function (doc) {
    return doc.ns.split('.')[1];
};




OpLogWrite.prototype.writeRecord = function (doc, op) {
    try {
        var self = this;
        var future = new Future();

        var ret = false;

        //var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = '';

        var tableName = self.getCollectionName(doc);

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);


        /*
        cmdMgr.ee.on('beforeExecSql', function (tableName, sql, doc, action) {
            self.writeDbLog(tableName, sql, doc, action);
        });
         */

        var sql = cmdMgr.prepareSql(tableName, doc, op).wait();
        var record = cmdMgr.prepareRecord(tableName, doc, op).wait();
        self.writeDbLog(tableName, sql, record, op).wait();


        /*
        sql = cmdMgr.prepareSql(tableName, doc, op).wait();
        future.return(sql != '' ? cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
         */

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

    var future = new Future;
    console.log('writeDbLog');
    self.syncdblog.insert({
        coll: tableName,
        oper: action,
        data: doc,
        command: sql,
        status: "",
        created: {ts: new Date(), app: "integrator"}
    });

    future.return(true);


    return future.wait();
}.future();


OpLogWrite.prototype.observeSyncDbLog = function (status) {
    if (status)
        this.timerObserveSyncDbLog = setInterval(processSyncDbLog, 5000, this);
    else
        clearInterval(this.timerObserveSyncDbLog);
}

var processSyncDbLog = function (context) {
    var self = context;

    try {
        var future = new Future;

        console.log("processSyncDbLog");
        if (self.connection.dbInstance != null) {
            self.observeSyncDbLog(false);
            var command = new SequelizeCommand(self.connection);
            self.syncdblog.find({status: ""}, {sort: {"created.ts": 1}}).forEach(function (doc) {
                var err = command.execSql(doc.command, doc.data, doc.oper).wait();
                self.syncdblog.update(
                    {_id: doc['_id']},
                    {$set: {status: err == null ? "OK" : "KO", errorMsg: err == null ? '' : err}});
            });
            self.observeSyncDbLog(true);
            future.return(true);
        }
        else {
            self.observeSyncDbLog(false);
            future.return(false);
        }
    }
    catch (e) {
        console.log(e);
        self.observeSyncDbLog(self.connection.dbInstance != null);
        future.return(false);
    }

    return future.wait();
}.future();


