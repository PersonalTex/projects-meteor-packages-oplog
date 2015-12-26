var Future = Npm.require('fibers/future');
var Fiber = Npm.require("fibers");
var Async = Npm.require("async");
 Log = function(error, msg) {
    console.log("Error "+error+ " msg "+msg);

}

/**
 * Created by paolo on 06/12/15.
 */
Tinytest.add('OpLog', function (test) {

    test.isNotNull(connManager.open('link').wait());
    /*
    Meteor.call('dbOpen','TEX', function(err, response) {
        test.equal(response, false, err);
    })
    */

});

