Package.describe({
  name: "link:oplog",
    version: "1.0.3",
  // Brief, one-line summary of the package.
  summary: "",
  // URL to the Git repository containing the source code for this package.
  git: "",
  // By default, Meteor will default to using README.md for documentation.
  // To avoid submitting documentation, set this field to null.
  documentation: "README.md"
});


/* This lets you use npm packages in your package*/
Npm.depends({
    "asserts" : "4.0.2",
    "mongodb": "2.0.48",
    "mongo-oplog": "1.0.1",
    //"fibers": "1.0.5",
    //"future": "2.3.1"
});

Package.onUse(function(api) {
  api.versionsFrom("1.2.1");
  api.use("ecmascript");
  api.use("underscore", "server");
  api.add_files('server/oplog.js', ["server"]);
  api.imply("link:dbaccess", ["server"]);
  api.export("OpLogEvents", ["server"]);
  api.export("OpLogWrite", ["server"]);
});

/*
Package.onTest(function(api) {

  api.use("ecmascript");
  api.use("tinytest",["server"]);
  api.use("link:oplog", ["server"]);
  api.add_files('./Test.js', ["server"]);
  api.add_files('server/oplog.js', ["server"]);

 });
 */