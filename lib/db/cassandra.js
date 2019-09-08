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
	var statement = "insert into samples(shardid, uuid, fingerprint, string, value) ";
       	db
       	 .beginQuery()
       	 .query(statement)
       	 .consistency("one")
       	 .options({ queryName: 'myColumnFamilySelect' })
       	 .options({ executeAsPrepared: true });

	// Add shardid, uuid
	db
	 .param('toDate(NOW())', 'ascii' )
	 .param('now()', 'ascii' );;
	// add find  
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
	var statement = "INSERT INTO time_series(fingerprint, labels, name)";
  	db
        .beginQuery()
        .query(statement)
        .consistency("one")
        .options({ queryName: 'insertIntoTimeSeries' })
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
          if (debug) console.log(statement,value.list,'Returned data: ' + data);
       });
  } 
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
	var dbKeyspace  = "CREATE KEYSPACE IF NOT EXISTS "+dbName+" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;";
	var ts_table = "CREATE TABLE IF NOT EXISTS "+dbName+".time_series (fingerprint bigint, labels varchar, name varchar, PRIMARY KEY (name, labels)) WITH CLUSTERING ORDER BY (labels ASC) AND bloom_filter_fp_chance = 0.01 AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} AND comment = 'timeseries' AND gc_grace_seconds = 60 AND compaction = {'compaction_window_size': '120','compaction_window_unit': 'MINUTES', 'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' };"
	var sm_table = "CREATE TABLE IF NOT EXISTS "+dbName+".samples (shardid varchar, uuid timeuuid, fingerprint bigint, value float, string varchar, PRIMARY KEY (shardid, uuid, fingerprint)) WITH CLUSTERING ORDER BY (uuid ASC, fingerprint ASC) AND bloom_filter_fp_chance = 0.01 AND caching = {'keys':'ALL', 'rows_per_partition':'NONE'} AND comment = 'samples' AND gc_grace_seconds = 345600 AND compaction = {'compaction_window_size': '120','compaction_window_unit': 'MINUTES', 'class': 'org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy' };"
	db.cql(
	  dbKeyspace,
	  [],
	  { consistency: db.consistencyLevel.one, queryName: 'dbKeySpace', executeAsPrepared: true },
	  function (err, data) {
	    if (err) {
	      console.log('ERROR: ' + err);
	      return;
	    }
	    if (debug) console.log('Created DB: ' + data);
		  db.cql(
			  ts_table,
			  [],
			  { consistency: db.consistencyLevel.one, queryName: 'dbKeySpace', executeAsPrepared: true },
			  function (err, data) {
			    if (err) {
			      console.log('ERROR: ' + err);
			      return;
			    }
			    if (debug) console.log('Created TS table: ' + data);
			  }
			);
		  db.cql(
			  sm_table,
			  [],
			  { consistency: db.consistencyLevel.one, queryName: 'dbKeySpace', executeAsPrepared: true },
			  function (err, data) {
			    if (err) {
			      console.log('ERROR: ' + err);
			      return;
			    }
			    if (debug) console.log('Created SM table: ' + data);
			  }
			);
	  }
	);
	reloadFingerprints();
};

var reloadFingerprints = function(){
  console.log('Reloading Fingerprints...');
  var rows = [];
  var select_query = "SELECT fingerprint, labels FROM time_series";
  /* TODO CASSA CREATE STATEMENT! */
  db
  .beginQuery()
  .query(select_query)
  .consistency("one")
  .options({ queryName: 'myColumnFamilySelect' })
  .options({ executeAsPrepared: true })
  .execute(function (err, data) {
    if (err) {
      console.log('ERROR: ' + err);
      return;
    }
    if (debug) console.log(select_query,'Returned data: ' + data);
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
			conditions.push(" '"+rule[0]+"' = '"+rule[2]+"' ")
		} else if (rule[1] == '!='){
			conditions.push(" '"+rule[0]+"') != '"+rule[2]+"' ")
		}
	});

	var finger_search = "SELECT fingerprint FROM time_series WHERE "+conditions.join('OR');
	if (debug) console.log('FINGERPRINT QUERY',finger_search);
	var finger_rows = [];
	  /* TODO CASSA CREATE STATEMENT! */
	  db
	  .beginQuery()
	  .query(finger_search)
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
			var select_query = "SELECT fingerprint, toUnixTimestamp(uuid) as timestamp_ms, string"
				+ " FROM samples"
				+ " WHERE fingerprint IN ("+finger_rows.join(',')+")"
				if (params.start && params.end) {
					select_query += " AND uuid > minTimeuuid( "+parseInt(params.start/1000000) +") AND uuid < maxTimeuuid("+parseInt(params.end/1000000)+")";
				}
				//select_query += " ORDER BY fingerprint, timestamp_ms"
			if (debug) console.log('SEARCH QUERY',select_query)
	    db
	      .beginQuery()
	      .query(select_query)
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
