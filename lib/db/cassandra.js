/*
 * cLoki DB Adapter for Cassandra
 * (C) 2018-2019 QXIP BV
 */

var debug = process.env.DEBUG || false;
var UTILS = require('../utils');
var toJSON = UTILS.toJSON;
var cassandra_options = {
  config: {
    cqlVersion: '3.0.0', /* optional, defaults to '3.1.0' */
    timeout: 4000, /* optional, defaults to 4000 */
    poolSize: 2, /* optional, defaults to 1 */
    consistencyLevel: 'one', /* optional, defaults to one. Will throw if not a valid Cassandra consistency level*/
    numRetries: 3, /* optional, defaults to 0. Retries occur on connection failures. Deprecated, use retryOptions instead. */
    retryDelay: 100, /* optional, defaults to 100ms. Used on consistency fallback retry */
    retryOptions: { retries: 0 }, /* optional. See https://www.npmjs.com/package/retry for options */
    enableConsistencyFailover: true, /* optional, defaults to true */
    coerceDataStaxTypes: false, /* optional, defaults to true */
    user: process.env.CASSANDRA_USER,
    password: process.env.CASSANDRA_PASS,
    keyspace: process.env.CASSANDRA_KEYSPACE, /* Default keyspace. Can be overwritten via options passed into #cql(), etc. */
    hosts: process.env.CASSANDRA_HOSTS,
    localDataCenter: process.env.CASSANDRA_DC /* required; used for selecting preferred nodes during load balancing */ 
  }
};

/* DB Helper */
var path = require('path');
var db = require('priam')(cassandra_options);
var ch;


/* Cache Helper */
var recordCache = require('record-cache');
var onStale = function(data){
 	for (let [key, value] of data.records.entries()) {
  
	     var statement = "INSERT INTO samples(fingerprint, timestamp_ms, value, string)";
       db
        .beginQuery()
        .query(statement)
        .consistency("one")
        .options({ queryName: 'myColumnFamilySelect' })
        .options({ executeAsPrepared: true });
        
       value.list.forEach(function(row){
		    if (!row.record) return;
		    db.param( row.record, 'ascii' );
       });
       
       db.execute(function (err, data) {
          if (err) {
            console.log('ERROR: ' + err);
            return;
          }
          console.log('Returned data: ' + data);
       });
  }
}
var onStale_labels = function(data){
 	for (let [key, value] of data.records.entries()) {
	     var statement = "INSERT INTO time_series(date, fingerprint, labels, name)";
   	   db
        .beginQuery()
        .query(statement)
        .consistency("one")
        .options({ queryName: 'myColumnFamilySelect' })
        .options({ executeAsPrepared: true });
        
       value.list.forEach(function(row){
		    if (!row.record) return;
		    db.param( row.record, 'ascii' );
       });
       
       db.execute(function (err, data) {
          if (err) {
            console.log('ERROR: ' + err);
            return;
          }
          console.log('Returned data: ' + data);
       });
}

// Flushing to Clickhouse
var bulk = recordCache({
  maxSize: process.env.BULK_MAXSIZE || 5000,
  maxAge: process.env.BULK_MAXAGE || 2000,
  onStale: onStale
})

var bulk_labels = recordCache({
  maxSize: 100,
  maxAge: 500,
  onStale: onStale_labels
})

// In-Memory LRU for quick lookups
var labels = recordCache({
  maxSize: process.env.BULK_MAXCACHE || 50000,
  maxAge: 0,
  onStale: false
})

/* Initialize */
var initialize = function(dbName){
	console.log('Initializing DB...');
	var dbQuery = "CREATE DATABASE IF NOT EXISTS "+dbName;
  /* TODO CASSA CREATE DB! */
  
  // Create Tables	
		var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (date Date,fingerprint UInt64,labels String, name String) ENGINE = ReplacingMergeTree PARTITION BY date ORDER BY fingerprint"
		var sm_table = "CREATE TABLE IF NOT EXISTS "+dbName+".samples (fingerprint UInt64,timestamp_ms Int64,value Float64,string String) ENGINE = MergeTree PARTITION BY toRelativeHourNum(toDateTime(timestamp_ms / 1000)) ORDER BY (fingerprint, timestamp_ms)"
    /* TODO CASSA CREATE TABLES! */
    reloadFingerprints();
	});
};

var reloadFingerprints = function(){
  console.log('Reloading Fingerprints...');
  var rows = [];
  var select_query = "SELECT DISTINCT fingerprint, labels FROM time_series";
  /* TODO CASSA CREATE STATEMENT! */
  db
  .beginQuery()
  .query('SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?')
  .param('value_of_keyCol1', 'ascii')
  .param('value_of_keyCol2', 'ascii')
  .consistency("one")
  .options({ queryName: 'myColumnFamilySelect' })
  .options({ executeAsPrepared: true })
  .execute(function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
    rows = data;
    rows.forEach(function(row){
		try {
			var JSON_labels = toJSON(row[1].replace(/\!?=/g,':'));
			labels.add(row[0],JSON.stringify(JSON_labels));
			for (var key in JSON_labels){
				if (debug) console.log('Adding key',row);
				labels.add('_LABELS_',key);
				labels.add(key,JSON_labels[key]);
			};

		} catch(e) { console.error(e); return }
    if (debug) console.log('Reloaded fingerprints:',rows.length+1);
    
  });

}

var scanFingerprints = function(JSON_labels,client,params,label_rules){
	if (debug) console.log('Scanning Fingerprints...',JSON_labels,label_rules);
	var resp = { "streams": [] };
	if (!JSON_labels) return resp;
	var conditions = [];
	if (debug) console.log('Parsing Rules...',label_rules);
	label_rules.forEach(function(rule){
		if (debug) console.log('Parsing Rule...',rule);
		if (rule[1] == '='){
			conditions.push("(visitParamExtractString(labels, '"+rule[0]+"') = '"+rule[2]+"')")
		} else if (rule[1] == '!='){
			conditions.push("(visitParamExtractString(labels, '"+rule[0]+"') != '"+rule[2]+"')")
		}
	});

	var finger_search = "SELECT DISTINCT fingerprint FROM time_series FINAL PREWHERE "+conditions.join('OR');
  if (debug) console.log('FINGERPRINT QUERY',finger_search);
	var finger_rows = [];
  /* TODO CASSA CREATE STATEMENT! */
  db
  .beginQuery()
  .query('SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?')
  .param('value_of_keyCol1', 'ascii')
  .param('value_of_keyCol2', 'ascii')
  .consistency("one")
  .options({ queryName: 'myColumnFamilySelect' })
  .options({ executeAsPrepared: true })
  .execute(function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    console.log('Returned data: ' + data);
    finger_rows = data;
		if (!finger_rows[0]) { client.send(resp); return; }

	  	if (debug) console.log('FOUND FINGERPRINTS: ', finger_rows);
	  	var select_query = "SELECT fingerprint, timestamp_ms, string"
			+ " FROM samples"
			+ " WHERE fingerprint IN ("+finger_rows.join(',')+")"
			if (params.start && params.end) {
				select_query += " AND timestamp_ms BETWEEN "+parseInt(params.start/1000000) +" AND "+parseInt(params.end/1000000)
			}
			select_query += " ORDER BY fingerprint, timestamp_ms"
		if (debug) console.log('SEARCH QUERY',select_query)
    db
      .beginQuery()
      .query('SELECT "myCol1", "myCol2" FROM "myColumnFamily" WHERE "keyCol1" = ? AND "keyCol2" = ?')
      .param('value_of_keyCol1', 'ascii')
      .param('value_of_keyCol2', 'ascii')
      .consistency("one")
      .options({ queryName: 'myColumnFamilySelect' })
      .options({ executeAsPrepared: true })
      .execute(function (err, data) {
        if (err) {
          console.log('ERROR: ' + err);
          return;
        }
		    var rows = data;
        if (debug) console.log('RESPONSE:',rows);
        var entries = [];
        rows.forEach(function(row){
        entries.push({ "timestamp": new Date(parseInt(row[1])).toISOString(), "line": row[2] })
        });
          resp.streams.push( { "labels": JSON.stringify(JSON_labels).replace(/:/g,'='), "entries": entries }  );
          client.send(resp);
      });
	});
}


/* Module Exports */

module.exports.database_options = cassandra_options;
module.exports.database = db;
module.exports.cache = { bulk: bulk, bulk_labels: bulk_labels, labels: labels };
module.exports.scanFingerprints = scanFingerprints;
module.exports.reloadFingerprints = reloadFingerprints;
module.exports.init = initialize;
