#! /usr/bin/env node

'use strict';

var tls = require('tls');
var net = require('net');
var eos = require('end-of-stream');
var through = require('through2');
var minimist = require('minimist');
var allContainers = require('docker-allcontainers');
var statsFactory = require('docker-stats');
var logFactory = require('docker-loghose');
var eventsFactory = require('docker-event-log');
var os = require('os');
var request = require('request');


function connect(opts) {
  var stream;
  if (opts.secure) {
    stream = tls.connect(opts.port, opts.server, onSecure);
  } else {
    stream = net.createConnection(opts.port, opts.server);
  }

  function onSecure() {
    // let's just crash if we are not secure
    if (!stream.authorized) throw new Error('secure connection not authorized');
  }

  return stream;
}


function start(opts) {
  console.log("Started successfully, streaming logs ...");

  var out;
  var noRestart = function() {};

  var filter = through.obj(function(obj, enc, cb) {
    addAll(opts.add, obj);
    var token = '';

    if (obj.line) {
      token = opts.logstoken;
    }
    else if (obj.type) {
      token = opts.eventstoken;
    }
    else if (obj.stats) {
      token = opts.statstoken;
    }

    var passes_regex = true;
    var regex = '';

    if (opts.matchByName != '') {
      regex = new RegExp(opts.matchByName)
      if (String(obj.name).match(regex) == null) {
        passes_regex = false;
      }
    }

    if (opts.matchByImage != '') {
      regex = new RegExp(opts.matchByImage)
      if (String(obj.image).match(regex) == null) {
        passes_regex = false;
      }
    }

    if (opts.skipByName != '') {
      regex = new RegExp(opts.skipByName)
      if (String(obj.name).match(regex) != null) {
        passes_regex = false;
      }
    }

    if (opts.skipByImage != '') {
      regex = new RegExp(opts.skipByImage)
      if (String(obj.image).match(regex) != null) {
        passes_regex = false;
      }
    }

    if (token && passes_regex == true) {
      this.push(token);
      this.push(' ');
      this.push(JSON.stringify(obj));
      this.push('\n');
    }

    cb()
  });

  var events = allContainers(opts);
  var loghose;
  var stats;
  var dockerEvents;
  var streamsOpened = 0;

  opts.events = events;

  if (opts.logs !== false && opts.logstoken) {
    loghose = logFactory(opts);
    loghose.pipe(filter);
    streamsOpened++;
  }

  if (opts.stats !== false && opts.statstoken) {
    stats = statsFactory(opts);
    stats.pipe(filter);
    streamsOpened++;
  }

  if (opts.dockerEvents !== false && opts.eventstoken) {
    dockerEvents = eventsFactory(opts);
    dockerEvents.pipe(filter);
    streamsOpened++;
  }

  if (!stats && !loghose && !dockerEvents) {
    throw new Error('You must provide a key for at least one of logs, stats, or dockerEvents');
  }

  pipe();

  // destroy out if all streams are destroyed
  loghose && eos(loghose, function() {
    streamsOpened--;
    streamClosed(streamsOpened);
  });
  stats && eos(stats, function() {
    streamsOpened--;
    streamClosed(streamsOpened);
  });
  dockerEvents && eos(dockerEvents, function() {
    streamsOpened--;
    streamClosed(streamsOpened);
  });

  return loghose;

  function addAll(proto, obj) {
    if (!proto) { return; }

    var key;
    for (key in proto) {
      if (proto.hasOwnProperty(key)) {
        obj[key] = proto[key];
      }
    }
  }

  function pipe() {
    if (out) {
      filter.unpipe(out);
    }

    out = connect(opts);

    filter.pipe(out, { end: false });

    // automatically reconnect on socket failure
    noRestart = eos(out, pipe);
  }

  function streamClosed(streamsOpened) {
    if (streamsOpened <= 0) {
      noRestart()
      out.destroy();
    }
  }
}


/**
 * Generate correct tokens to use for logs, stats, and docker events.
 * Allows user to specify one or moe of those tokens when invoking this script.
 * In absence of those tokens, service will attempt to retrieve the tokens
 * from Rancher's metadata service in the form of host labels. Host labels are:
 *     logentries-token         Base token
 *     logentries-token-logs    Token specifically for log output
 *     logentries-token-stats   Token specifically for Docker Stats output
 *     logentries-token-events  Token specifically for Docker Events output
 */
function generateTokens(opts) {

  // If user passes any tokens explicitly, then use those.
  if ( opts.token || opts.logstoken || opts.statstoken || opts.eventstoken ) {
    console.log("Using provided tokens ... ");
    opts.logstoken = opts.logstoken || opts.token || '';
    opts.statstoken = opts.statstoken || opts.token || '';
    opts.eventstoken = opts.eventstoken || opts.token || '';

    start(opts);

  } else {
    // Attempt to query Rancher's metadata service. Provide setTokenValues as
    // callback.
    console.log("Retrieving tokens from host labels ...");
    requestMetadata(setTokenValues);
  }

  /**
   * Make a request to Rancher's metadata service for this container's host's
   * labels. This requires a callback to be provided to execute upon receiving
   * the response, if any.
   */
  function requestMetadata(request_cb) {
      var labels_api = 'http://rancher-metadata/2015-12-19/self/host/labels';

      var request_options = {
        url: labels_api,
        headers: {
          'Accept': 'application/json'
        }
      };

      request(request_options, function (error, response, body) {
        if (!error && response.statusCode == 200) {
            try {
              request_cb(JSON.parse(body));
            } catch (e) {
              throw new Error('Unable to parse response from metadata service ...');
            }
        } else {
          throw new Error('Unable to connect to Rancher ...');
        }
      });
  }

  /**
   * Upon successfully retrieving host labels from Rancher's metadata service,
   * attempt to set the logstoken, statstoken, and eventstoken based on
   * labels that may exist on that host.
   * Start logger after this has been completed.
   */
  function setTokenValues(labels_data) {
    var token_base, token_logs, token_stats, token_events;

    for ( var k in labels_data ) {
      if ( k === 'logentries-token' ) {
        token_base = labels_data[k];
      } else if ( k === 'logentries-token-logs' ) {
        token_logs = labels_data[k];
      } else if ( k === 'logentries-token-stats' ) {
        token_stats = labels_data[k];
      } else if ( k === 'logentries-token-events' ) {
        token_events = labels_data[k];
      }
    }

    opts.logstoken = token_logs || token_base || '';
    opts.statstoken = token_stats || token_base || '';
    opts.eventstoken = token_events || token_base || '';

    start(opts);
  }
}


function cli() {
  var unbound;
  var argv = minimist(process.argv.slice(2), {
    boolean: ['json', 'secure', 'stats', 'logs', 'dockerEvents'],
    string: ['token', 'logstoken', 'statstoken', 'eventstoken', 'server', 'port'],
    alias: {
      'token': 't',
      'logstoken': 'l',
      'newline': 'n',
      'statstoken': 'k',
      'eventstoken': 'e',
      'secure': 's',
      'json': 'j',
      'statsinterval': 'i',
      'add': 'a'
    },
    default: {
      json: false,
      newline: true,
      stats: true,
      logs: true,
      dockerEvents: true,
      statsinterval: 30,
      add: [ 'host=' + os.hostname() ],
      token: process.env.LOGENTRIES_TOKEN,
      logstoken: process.env.LOGENTRIES_LOGSTOKEN || process.env.LOGENTRIES_TOKEN,
      statstoken: process.env.LOGENTRIES_STATSTOKEN || process.env.LOGENTRIES_TOKEN,
      eventstoken: process.env.LOGENTRIES_EVENTSTOKEN || process.env.LOGENTRIES_TOKEN,
      server: 'data.logentries.com',
      port: unbound,
      matchByImage: '',
      matchByName: '',
      skipByImage: '',
      skipByName: ''
    }
  });

  if (argv.help) {
    console.log('Usage: docker-logentries [-l LOGSTOKEN] [-k STATSTOKEN] [-e EVENTSTOKEN]\n' +
                '                         [-t TOKEN] [--secure] [--json]\n' +
                '                         [--no-newline] [--no-stats] [--no-logs] [--no-dockerEvents]\n' +
                '                         [-i STATSINTERVAL] [-a KEY=VALUE]\n' +
                '                         [--matchByImage REGEXP] [--matchByName REGEXP]\n' +
                '                         [--skipByImage REGEXP] [--skipByName REGEXP]\n' +
                '                         [--server HOSTNAME] [--port PORT]\n' +
                '                         [--help]\n' +
                '                         NOTE: All REGEXP must exclude the first and last \'/\' forward slashes.');

    process.exit(1);
  }

  if (argv.port == unbound) {
    if (argv.secure) {
      argv.port = 443;
    } else {
      argv.port = 80;
    }
  } else {
      argv.port = parseInt(argv.port);
      // TODO: support service names
      if (isNaN(argv.port)) {
        console.log('port must be a number');
        process.exit(1);
      }
  }

  if (argv.add && !Array.isArray(argv.add)) {
    argv.add = [argv.add];
  }

  argv.add = argv.add.reduce(function(acc, arg) {
    arg = arg.split('=');
    acc[arg[0]] = arg[1];
    return acc
  }, {});

  // Create tokens used for logging logs, stats, and events, then start logger
  generateTokens(argv);

}


module.exports = start;

if (require.main === module) {
  console.log("Starting logentries exporter v0.3.0 ...")
  cli();
}
