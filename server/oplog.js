var MongoOplog = Npm.require('mongo-oplog');


OpLogEvents = function(uri, filter, options) {
    this.uri = uri;
    this.filter = filter;
    this.options = options;
}

OpLogEvents.prototype.run = function() {
    try {
        var self = this;

        var oplog = MongoOplog(self.uri, {ns: self.filter}).tail();

        oplog.on('op', function (data) {
            //console.log(data);
        });

        oplog.on('insert', function (doc) {
            //console.log(doc.op);
            console.log(doc.op);
            self.insert(doc);
        });

        oplog.on('update', function (doc) {
            console.log(doc.op);
            self.update(doc);

        });

        oplog.on('delete', function (doc) {
            console.log(doc.op);
            self.delete(doc);
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


OpLogEvents.prototype.insert = function(doc) {
}

OpLogEvents.prototype.update = function(doc) {
}

OpLogEvents.prototype.delete = function(doc) {
}

OpLogEvents.prototype.getCollectionName = function(doc) {
    return doc.ns.split('.')[1];
}



OpLogWrite = function(uri, filter, options) {
    OpLogEvents.call(this, uri, filter, options);

}


OpLogWrite.prototype = Object.create(OpLogEvents.prototype);



OpLogWrite.prototype.insert = function(doc) {
    var future = new Future();
    console.log('insert ' + doc.o__id.toString());
    var sql = this.options.commandManager.prepareInsert(this.getCollectionName(doc), doc);
    console.log(sql);


}.future()

OpLogWrite.prototype.update = function(doc) {
    console.log('update '+doc.o2._id.toString());
    var sql = this.options.commandManager.prepareUpdate(this.getCollectionName(doc), doc);

    console.log(sql);
}

OpLogWrite.prototype.delete = function(doc) {
    console.log('delete '+doc.o._id.toString()());
    var sql = this.options.commandManager.prepareDelete(this.getCollectionName(doc), doc);

    console.log(sql);

}



