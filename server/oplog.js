"use strict"

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
            self.writeRecord(doc, 'i').wait();
        }.future());

        oplog.on('update', function (doc) {
            self.writeRecord(doc, 'u').wait();

        }.future());

        oplog.on('delete', function (doc) {
            self.writeRecord(doc, 'd').wait();
        }.future());

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
};

OpLogWrite.prototype = Object.create(OpLogEvents.prototype);


OpLogWrite.prototype.writeRecord = function (doc, op) {
    try {
        var self = this;
        var future = new Future();
        var ret = false;

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = '';

        var tableName = self.getCollectionName(doc);
        switch (op) {
            case 'i':
                sql = cmdMgr.prepareInsert(tableName, doc);
                break;
            case 'u':
                sql = cmdMgr.prepareUpdate(tableName, doc);
                break;
            case 'd':
                sql = cmdMgr.prepareDelete(tableName, doc);
                break;
        }
        ret = sql != '' ? cmdMgr.execSql(sql, tableName, doc, op).wait() : true;
    }
    catch (e) {
        console.log(e);
    }
    finally {
        future.return(ret);
        return future.wait();

    }
}.future();

