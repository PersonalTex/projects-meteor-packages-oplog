//var Future = Npm.require('fibers/future');

/**
 * Created by paolo on 06/12/15.
 */


var commandMgr = null;
var dbDef = null;
var connManager = null;


Tinytest.add('OpLog', function (test) {
    var uri =  'mongodb://127.0.0.1:1961/link';
    var filter = '(^link.doc)';
    var options = {};

    try {
    if(dbDef == null)
      dbDef =  new DbDef(Meteor.settings.Def);


    if(commandMgr == null) {
        commandMgr = new SequelizeCommandManager(dbDef);

        options.commandManager = commandMgr;
    }

    if(connManager == null)
      connManager = new DbConnectionManager(Meteor.settings.DbConnections);

    connManager.open(

    options.legacyConnection = connManager.open('MSSQL').wait());

        var op = new OpLogWrite(uri, filter, options);
        op.run();

        test.isNotNull(op);
    }
    catch(e) {
        test.isNotNull(null);
    }
    /*
    Meteor.call('dbOpen','TEX', function(err, response) {
        test.equal(response, false, err);
    })
    */

});

