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



OpLogWrite = function(uri, filter, options) {
    OpLogEvents.call(this, uri, filter, options);
}

OpLogWrite.prototype = Object.create(OpLogEvents.prototype);


OpLogWrite.prototype.insert = function(doc) {
    console.log('insert '+doc._id)
}

OpLogEvents.prototype.update = function(doc) {
    console.log('update '+doc._id)
}

OpLogEvents.prototype.delete = function(doc) {
    console.log('delete '+doc._id)
}



