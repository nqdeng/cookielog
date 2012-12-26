/**
 * Cookie log combination script.
 * Nanqiao Deng <nanqiao.dengnq@alibaba-inc.com>
 * MIT Licensed
 */

/* Performance profiling. */

process.on('exit', function (start) {
	console.log('Total time: %sms', Date.now() - start);
}.bind(null, Date.now()));

/* Main code here. */

var fs = require('fs'),
	fsb = process.binding('fs');

var	EOF = {},

	PAUSE = {},

	CIRCLE = {},

	EMPTY = new Buffer(0),

	DELAY = 30, // Allow passengers to be late in 30 minitues.

	INTERVAL = 5, // Collect passengers in 5 minitues each time.

	BLOCK_SIZE = 1024 * 1024 * 8, // Default size of one carriage.

	carriages = [],

	ptr = Number.POSITIVE_INFINITY,

	/**
	 * Put passengers into corresponding carriages.
	 * @param line {string}
	 * @param index {number}
	 */
	cache = function (line, index) {
		var buffer = carriages[index];

		if (buffer === null) { // Missed the train.
			console.error('Ignore noise data: %s', line);
			return;
		} else {
			if (!buffer) {
				buffer = extend(index);
			}

			while (buffer.ptr + line.length > buffer.length) {
				buffer = extend(index);
			}

			line.copy(buffer, buffer.ptr);
			buffer.ptr += line.length;
		}

		if (index < ptr) { // Point to the first carriage in waiting.
			ptr = index;
		}
	},

	/**
	 * Extend carriage size.
	 * @param index {number}
	 * @return {Object}
	 */
	extend = function (index) {
		var buffer, tmp;

		if (!carriages[index]) {
			buffer = carriages[index] = new Buffer(BLOCK_SIZE);
			buffer.ptr = 0;
		} else {
			tmp = carriages[index];
			buffer = carriages[index] = new Buffer(tmp.length + BLOCK_SIZE);
			tmp.copy(buffer);
			buffer.ptr = tmp.ptr;
		}

		return buffer;
	},

	/**
	 * Find initial index.
	 * @param pathnames {Array}
	 * @return {number}
	 */
	findBase = function (pathnames) {
		var indexes = pathnames.map(function (pathname) {
				var buffer = new Buffer(64),
					fd = fs.openSync(pathname, 'r');

				fs.readSync(fd, buffer, 0, buffer.length);
				fs.closeSync(fd);

				return toIndex(buffer);
			});

		return Math.min.apply(Math, indexes);
	},

	/**
	 * Generate train station.
	 * @param pathname01 {string}
	 * @param pathname30 {string}
	 * @return {Function}
	 */
	flush = function (pathname01, pathname30) {
		var writer = writeAll(pathname01, pathname30);

		return function (signal) {
			if (signal === CIRCLE) { // Send parts of carriages.
				writer(carriages.length - DELAY);
			} else if (signal === EOF) { // Send all carriages.
				writer(carriages.length);
			}
		};
	},

	/**
	 * Start transporting passengers.
	 * @param source {Array}
	 * @param target01 {string}
	 * @param target30 {string}
	 */
	main = function (sources, target01, target30) {
		var base = findBase(sources),
			reader = readAll(sources, base, INTERVAL),
			flusher = flush(target01, target30);

		reader(flusher);
	},

	/**
	 * Generate the final tunnel.
	 * @param pathnames {Array}
	 * @param threshold {number}
	 * @param step {number}
	 * @return {Function}
	 */
	readAll = function (pathnames, threshold, step) {
		var readers = pathnames.map(function (pathname) {
				return readOne(pathname, threshold, step);
			});

		return function (callback) {
			(function next(i) { // Collect passengers from each sub tunnel.
				if (i < readers.length) {
					readers[i](function (signal) {
						if (signal === PAUSE) {
							next(i + 1);
						} else if (signal === EOF) {
							readers.splice(i, 1);
							next(i);
						}
					});
				} else {
					if (readers.length > 0) {
						callback(CIRCLE);
						next(0);
					} else {
						callback(EOF);
					}
				}
			}(0));
		};
	},

	/**
	 * Generate the sub tunnel.
	 * @param pathnames {Array}
	 * @param threshold {number}
	 * @param step {number}
	 * @return {Function}
	 */
	readOne = function (pathname, threshold, step) {
		var fd = fs.openSync(pathname, 'r'),
			bufferSize = 1024 * 64,
			buffer = new Buffer(bufferSize),
			piece = EMPTY,
			pause = false,
			queue = [],
			working = false,
			eof = false,
			threshold, callback;

		var dup = function (source, start, end) {
				var ret = new Buffer(end - start);

				source.copy(ret);

				return ret;
			},

			mix = function (target, source, start, end) {
				var ret;

				if (target.length === 0) {
					return source.slice(start, end);
				} else {
					ret = new Buffer(target.length + end - start);
					target.copy(ret);
					source.copy(ret, target.length, start, end);

					return ret;
				}
			},

			read = function () {
				fsb.read(fd, buffer, 0, bufferSize, null, function (err, size) {
					if (err) {
						throw err;
					} else if (size === 0) {
						fs.closeSync(fd);
						eof = true;
						trigger();
					} else {
						if (pause) {
							queue.push(dup(buffer, 0, size));
							callback(PAUSE);
							trigger();
						} else {
							queue.push(dup(buffer, 0, size));
							read();
							trigger();
						}
					}
				});
			},

			trigger = function () {
				if (!working) {
					working = true;

					while (queue.length > 0) {
						var data = queue.shift(),
							start = 0,
							end = 0,
							len = data.length,
							line, index;

						while (end < len) {
							while (end < len && data[end++] !== 10); // '\n'
							if (end === len) {
								piece = mix(piece, data, start, end);
							} else {
								line = mix(piece, data, start, end);
								piece = EMPTY;
								start = end;

								if (line.length > 0 && line[0] !== 10) { // Skip empty line.
									index = toIndex(line);
									cache(line, index);
									pause = pause || index > threshold;
								}
							}
						}
					}

					if (eof) {
						if (piece.length > 0 && piece[0] !== 10) { // Skip empty line.
							if (piece[piece.length - 1] !== 10) { // Auto append LF.
								piece = mix(piece, new Buffer([ 10 ]), 0, 1);
							}
							cache(piece, toIndex(piece));
						}
						callback(EOF);
					}

					working = false;
				}
			};

		return function (fn) {
			pause = false;
			threshold += step;
			callback = fn;

			read();
		};
	},

	/**
	 * Convert index to date string.
	 * @param index {number}
	 * @return {string}
	 */
	toDate = (function () {
		var PATTERN = /^\w+ (\w+) (\d+) (\d+) (\d+:\d+):.*$/

		return function (index) {
			return new Date(index * 60000).toString().replace(PATTERN, '$2/$1/$3:$4\n');
		};
	}()),

	/**
	 * Convert date string in one line to index.
	 * @param line {string}
	 * @return {number}
	 */
	toIndex = (function () {
		var PATTERN = /^(.*?)\/(.*?)\/(.*?):(.*)$/,
			cache = {};

		return function (line) {
			var len = line.length,
				start = 0,
				end, date;

			while (start < len && line[start++] !== 91); // '['

			if (start === len) {
				throw new Error('invalid line: ' + line.toString('ascii'));
			} else {
				end = start + 17;
				date = line.toString('ascii', start, end);
			}

			if (!cache[date]) {
				cache[date] =
					new Date(date.replace(PATTERN, '$1 $2 $3 $4')).getTime() / 60000;
			}

			return cache[date];
		};
	}()),

	/**
	 * Generate the destination.
	 * @param pathname01 {string}
	 * @param pathname30 {string}
	 * @return {Function}
	 */
	writeAll = function (pathname01, pathname30) {
		var writer01 = writeOne(pathname01),
			writer30 = writeOne(pathname30),
			count = 0,
			terminal = 0,
			working = false;

		return function (pos) {
			if (terminal < pos) {
				terminal = pos;
			}

			if (!working) {
				working = true;

				while (ptr < terminal) {
					var stamp = new Buffer(toDate(ptr), 'ascii'),
						data = carriages[ptr];

					if (data) {
						data = data.slice(0, data.ptr);
					} else {
						data = EMPTY;
					}

					carriages[ptr++] = null;

					writer01(stamp);
					writer01(data);
					if (count++ % 30 === 0) {
						writer30(stamp);
					}
					writer30(data);
				}

				if (terminal === carriages.length) {
					writer01(EOF);
					writer30(EOF);
				}

				working = false;
			}
		};
	},

	/**
	 * Generate sub destination.
	 * @param pathname {string}
	 * @return {Function}
	 */
	writeOne = function (pathname) {
		var fd = fs.openSync(pathname, 'w'),
			queue = [],
			working = false;

		var write = function (buffer, callback) {
				fsb.write(fd, buffer, 0, buffer.length, null, function (err, size) {
					if (err) {
						throw err;
					} else if (size < buffer.length) {
						write(buffer.slice(size), callback);
					} else {
						callback();
					}
				});
			},

			trigger = function () {
				if (!working) {
					working = true;

					(function next() {
						if (queue.length > 0) {
							var data = queue.shift();

							if (data === EOF) {
								fs.closeSync(fd);
							} else if (data === EMPTY) {
								next();
							} else {
								write(data, next);
							}
						} else {
							working = false;
						}
					}());
				}
			};

		return function (data) {
			queue.push(data);
			trigger();
		};
	};

// Start program.
main(
	[
		'sample1',
		'sample2',
	],
	'cookie_log_total_1',
	'cookie_log_total_30'
);
