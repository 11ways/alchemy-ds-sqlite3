var sqlite3 = alchemy.use('sqlite3'),
    libpath = alchemy.use('path'),
    Mosql = alchemy.use('mongo-sql'),
    bson = alchemy.use('bson');

/**
 * SQLite3 Datasource, based on MongoDB
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
var SqliteDS = Function.inherits('Alchemy.Datasource.Sql', function Sqlite3(name, _options) {

	var options,
	    uri;

	// Define default options
	this.options = {
		path: null
	};

	Sqlite3.super.call(this, name, _options);

	if (!this.options.path) {
		this.options.path = ':memory:';
		log.warn('Could not find path for', name, 'Sqlite3 database, storing in-memory');
	}

	// Cache collections in here
	this.collections = {};
});

/**
 * Get table info
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   table_name
 *
 * @return   {Pledge}
 */
SqliteDS.setMethod(function getTableInfo(table_name) {

	var pledge = new Pledge(),
	    sql = 'PRAGMA table_info(' + this.escapeName(table_name) + ');';

	this.execQuery(sql).done(function gotResult(err, rows) {

		if (err) {
			return pledge.reject(err);
		}

		let info = {},
		    row,
		    i;

		for (i = 0; i < rows.length; i++) {
			row = rows[i];

			info[row.name] = {
				name    : row.name,
				type    : row.type,
				null    : !row.notnull,
				default : row.dflt_value,
				primary : !row.pk,

				// What's a cid?
				cid     : row.cid
			};
		}

		pledge.resolve(info);
	});

	return pledge;
});

/**
 * Execute a query command without result
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   sql
 * @param    {Array}    params
 *
 * @return   {Pledge}
 */
SqliteDS.setMethod(function queryCommand(sql, params) {
	return this.execQuery(sql, params, true);
});

/**
 * Execute a query and return all rows
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   sql
 * @param    {Array}    params
 *
 * @return   {Pledge}
 */
SqliteDS.setMethod(function queryAll(sql, params) {
	return this.execQuery(sql, params, false);
});

/**
 * Execute a query
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 *
 * @param    {String}   sql
 * @param    {Array}    params
 * @param    {Boolean}  run      Don't get result data but return statement
 *
 * @return   {Pledge}
 */
SqliteDS.setMethod(function execQuery(sql, params, run) {

	var that = this;

	let pledge = Function.series(this.connect(), function gotDb(next, db) {

		let cmd = db.prepare(sql, params);

		if (run) {
			return cmd.run(function done(err, result) {
				next(err, this);
			});
		}

		cmd.all(function done(err, result) {

			if (err) {
				return next(err);
			}

			next(null, result);
		});

	}, function done(err, result) {

		if (err) {
			return;
		}

		return result[1];
	});

	return pledge;
});

/**
 * Get a connection to the database
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
SqliteDS.decorateMethod(Blast.Decorators.memoize({ignore_arguments: true}), function connect() {

	var that = this,
	    pledge = new Pledge();

	let db = new sqlite3.Database(this.options.path, function connected(err) {

		if (err) {
			pledge.reject(err);
		} else {
			pledge.resolve(db);
		}
	});

	return pledge;
});

/**
 * Convert database values to app
 *
 * @author   Jelle De Loecker   <jelle@develry.be>
 * @since    0.1.0
 * @version  0.1.0
 */
SqliteDS.setMethod(function _valueToApp(field, query, options, value, callback) {

	if (field.datatype == 'string' && value) {
		// Some sqlite3 databases terminate their strings with null
		if (value.charCodeAt(value.length - 1) === 0) {
			value = value.slice(0, value.length - 1);
		}
	}

	Blast.setImmediate(function immediateDelay() {
		callback(null, value);
	});
});