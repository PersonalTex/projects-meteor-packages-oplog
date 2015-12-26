var MongoOplog = require('mongo-oplog');

module.exports = (function(){
    var OpLogEvents = function(dbName, filter, connectionString) {
       try {
            var self = this;
            this.linkSql = new LinkSql();
            this.dbName = Application.locals.DBCONNECTION.getConfig(alias).db;
            this.filter = '(^'+this.dbName+'.doc)';
            //var oplog = MongoOplog(Application.locals.DBCONNECTION.getConnectionString(alias), {ns: this.filter}).tail();
            var oplog = MongoOplog(connectionString, {ns: this.filter}).tail();

            oplog.on('op', function (data) {
                console.log(data);
            });

            oplog.on('insert', function (doc) {
                console.log(doc.op);
            });

            oplog.on('update', function (doc) {
                console.log(doc.op);

            });

            oplog.on('delete', function (doc) {
                console.log(doc.op._id);
            });

            oplog.on('error', function (error) {
                console.log(error);
                throw error;
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
            console.log("OpLogEvents creation failed")
            throw error;

        }

    };
    return OpLogEvents;

})();




