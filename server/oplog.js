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
            self.writeRecord(doc, 'i').detach();
        }.future());

        oplog.on('update', function (doc) {
            self.writeRecord(doc, 'u').detach();

        }.future());

        oplog.on('delete', function (doc) {
            self.writeRecord(doc, 'd').detach();
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
    this.counters = {ins: 0, upd: 0, del: 0, err: 0};

    this.cmdMgr = new OpSequelizeCommandManager(connection, dbTables);

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

        sql = self.cmdMgr.prepareSql(tableName, doc, op).wait();
        future.return(sql != '' ? self.cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
        /*
        switch (op) {
            case 'i':
         sql = self.cmdMgr.prepareInsert(tableName, doc).wait();
         future.return(sql != '' ? self.cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
                break;
            case 'u':
         sql = self.cmdMgr.prepareUpdate(tableName, doc).wait();
         future.return(sql != '' ? self.cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
                break;
            case 'd':
         sql = self.cmdMgr.prepareDelete(tableName, doc).wait();
         future.return(sql != '' ? self.cmdMgr.execSql(sql, tableName, doc, op).wait() : true);
                break;
        }
         */
        //ret = sql != '' ? cmdMgr.execSql(sql, tableName, doc, op).wait() : true;
        /*
         if(!ret)
         self.counters.err++;
         else if(op == 'i')
         self.counters.ins++;
         else if(op == 'u')
         self.counters.upd++;
         else if(op == 'd')
         self.counters.del++;

         future.return(ret);
         */
        return future.wait();


    }
    catch (e) {
        console.log(e);
        future.return(false);
        return future.wait();
    }
}.future();