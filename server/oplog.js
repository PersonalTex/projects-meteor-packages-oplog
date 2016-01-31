var MongoOplog = Npm.require('mongo-oplog');
var Future = Npm.require('fibers/future');
var util = Npm.require('util');


var USE_SYNC_ACTIVITY = true;
/*
 OpLogWrite
 */

/*
OpLogWrite = function (uri, filter, connection, dbTables) {


 this.uri = uri;
    this.filter = filter;
    this.dbTables = dbTables;
    this.connection = connection;
    timerObserveSyncDbLog = -1;
 timerObserveLegacyConn = -1

    this.syncdblog = new Mongo.Collection('syncdb_log');
    //this.cmdMgr = new OpSequelizeCommandManager(connection, dbTables);
}
 */

OpLogWrite = function (connMgr, confColl) {
    try {
        this.connMgr = connMgr;
        this.conf = confColl;
        this.localUri = "";
        this.filter = "";
        this.dbTables = null;
        this.legacyConnection = null;
        this.localConnection = null;
        this.timerObserveSyncDbLog = -1;
        this.timerObserveLegacyConn = -1;
        this.localAlias = this.conf.OpLog.Databases.local;
        this.inited = false;
        this.legacyAlias = this.conf.OpLog.Databases.legacy;
        this.syncdblog = new Mongo.Collection('syncdb_log');
    }
    catch (e) {
        console.log(e);
    }


}
OpLogWrite.prototype.init = function () {
    var self = this;

    try {
        var future = new Future();

        this.localUri = self.connMgr.getConnectionString(self.localAlias);
        this.filter = util.format('(^%s.doc)', self.conf.DbConnections[self.localAlias].db);
        this.legacyConnection = self.connMgr.get(self.legacyAlias).wait();
        this.localConnection = self.connMgr.get(self.localAlias).wait();

        /// serve wait ???
        self.dbTables = new DbCollectionUtil(self.conf.Def.Collections, self.localConnection);
        ///
        self.dbTables.init().wait();
        self.inited = true;
        future.return(true);

    }
    catch (e) {
        console.log(e);
        self.inited = false;
        future.return(false);
    }
    return future.wait();


}.future();

OpLogWrite.prototype.start = function () {
    try {
        var self = this;

        if (!self.inited)
            self.init().wait();

        if (USE_SYNC_ACTIVITY) {
            this.observeSyncDbLog(true);
        }

        var oplog = MongoOplog(self.localUri, {ns: self.filter}).tail();

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
        console.log("OpLogWrite creation failed");
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


        var tableName = self.getCollectionName(doc);
        var cmdMgr = new OpSequelizeCommandManager(self.connMgr.get(self.legacyAlias).wait(), self.dbTables);


        /*
         cmdMgr.ee.on('beforeExecSql', function (tableName, sql, doc, action) {
         self.writeDbLog(tableName, sql, doc, action);
         });
         */

        if (USE_SYNC_ACTIVITY) {
            var sql = cmdMgr.prepareSql(tableName, doc, op).wait();
            var record = cmdMgr.prepareRecord(tableName, doc, op).wait();
            self.writeDbLog(tableName, sql, record, op).wait();
        }
        else {
            var sql = cmdMgr.prepareSql(tableName, doc, op).wait();
            //future.return(sql != '' ? cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
            if (sql != "") {
                var err = cmdMgr.execSql(sql, tableName, doc, op).wait();
            }
        }

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
    if (status) {
        // disable observeLegacyConn
        this.observeLegacyConn(false);
        if (this.timerObserveSyncDbLog == -1) {
            this.timerObserveSyncDbLog = setInterval(processSyncDbLog, 2000, this);
        }
    }
    else if (this.timerObserveSyncDbLog != -1) {
        clearInterval(this.timerObserveSyncDbLog);
        this.timerObserveSyncDbLog = -1;
    }
}


OpLogWrite.prototype.observeLegacyConn = function (status) {
    if (status) {
        // disable observeSyncDbLog
        this.observeSyncDbLog(false);
        if (this.timerObserveLegacyConn == -1)
            this.timerObserveLegacyConn = setInterval(checkLegacyConn, 10000, this);

    }
    else if (this.timerObserveLegacyConn != -1) {
        clearInterval(this.timerObserveLegacyConn);
        this.timerObserveLegacyConn = -1;
    }
}

var processSyncDbLog = function (context) {
    var self = context;

    self.observeSyncDbLog(false);

    try {
        var future = new Future;


        console.log("processSyncDbLog");
        var err = null;
        if (self.legacyConnection.dbInstance != null) {
            var command = new SequelizeCommand(self.legacyConnection);

            self.syncdblog.find({status: ""}, {sort: {"created.ts": 1}, limit: 1000}).forEach(function (doc) {
                err = command.execSql(doc.command, doc.data, doc.oper).wait();

                // connection refuse throw exception
                if ((err != null && err.connectionRefused())) {
                    self.legacyConnection.dbInstance = null;
                    self.observeLegacyConn(true);
                    throw (err.message);
                }

                self.syncdblog.update(
                    {_id: doc['_id']},
                    {$set: {status: err == null ? "OK" : "KO", errorMsg: err == null ? '' : err.message}});
            });
            self.observeSyncDbLog(true);
            future.return(true);
        }
        else {
            self.observeLegacyConn(true);
            future.return(false);
        }

    }
    catch (e) {
        console.log(e);
        self.observeSyncDbLog(self.legacyConnection.dbInstance != null);
        future.return(false);
    }

    return future.wait();
}.future();


var checkLegacyConn = function (context) {
    var self = context;

    self.observeLegacyConn(false);

    try {
        var future = new Future;

        console.log("checkLegacyConn");

        var conn = self.connMgr.open(self.legacyAlias).wait();
        if (conn.dbInstance != null) {
            self.observeSyncDbLog(true);
        }
        else
            self.observeLegacyConn(true);
        future.return(conn.dbInstance != null);

    }
    catch (e) {
        console.log(e);
        // cosa fare?
        self.observeLegacyConn(true);
        future.return(false);
    }

    return future.wait();
}.future();


