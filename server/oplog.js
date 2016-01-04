//"use strict";

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
            console.log(doc.op + ' ' + doc['_id']);
            self.ins(doc);

        })

        oplog.on('update', function (doc) {
            //console.log(doc);
            console.log(doc.op + ' ' + doc.o2['_id']);
            //self.upd(doc);

        });

        oplog.on('delete', function (doc) {
            console.log(doc.op + ' ' + doc.o['_id']);
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



OpLogWrite.prototype.ins = function (doc) {
    try {

        var self = this;


        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = cmdMgr.prepareInsert(this.getCollectionName(doc), doc);

        if (sql != '') {
            var ret = cmdMgr.execSql(sql, self.getCollectionName(doc), doc, 'i').wait();
        }
    }
    catch (e) {
        console.log(e);

    }

}.future();


OpLogWrite.prototype.upd = function (doc) {
    try {

        var self = this;

        //future = new Future();

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = cmdMgr.prepareUpdate(this.getCollectionName(doc), doc);

        if (sql != '') {

            var ret = cmdMgr.execSql(sql, self.getCollectionName(doc), doc, 'u').wait();
            //console.log(ret == true ? "Update success " + doc.o._id.toString() : "Update fail " + doc.o._id.toString());
        }
    }

    catch (e) {
        console.log(e);

    }


}.future();


OpLogWrite.prototype.del = function (doc) {
    try {

        var self = this;

        var cmdMgr = new OpSequelizeCommandManager(self.connection, self.dbTables);

        var sql = cmdMgr.prepareDelete(this.getCollectionName(doc), doc);

        if (sql != '') {

            var ret = cmdMgr.execSql(sql, self.getCollectionName(doc), doc, 'd').wait();
            //console.log(ret == true ? "Delete success " + doc.o._id.toString() : "Delete fail " + doc.o._id.toString());
        }
    }
    catch (e) {
        console.log(e);

    }

}.future();



