//var Future = Npm.require('fibers/future');

/**
 * Created by paolo on 06/12/15.
 */
Tinytest.add('OpLog', function (test) {
    var uri =  'mongodb://127.0.0.1:1961/link';
    var filter = '(^link.doc)';

    try {
        var op = new OpLogWrite(uri, filter);
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

