async	= require "async"
log	= require( "logging" ).from __filename
url	= require "url"
http	= require "http"
util	= require "util"

# A list of stations we want to query..
stations = [ { name: "Winnipeg at The Forks", id: 28051 } ]

# The years and months we want to grab.
years	= [ 2013 ]

# Note that we build up the months objects
# with simple logic about how many days.
months	= []
for i in [1...12]
	if i in [ 9, 4, 6, 11 ]
		months.push { month: i, days: 30 }
	else if i is 2
		months.push { month: i, days: 28 }
	else
		months.push { month: i, days: 31 }

# Some base arguments for the request.
args = { "format": "csv", "timeframe": 2, "submit": "Download Data" }

_options = [ ]

# Bulid up an array of objects that are used for the request.
# Not the most effecient.. but it works and allows for
# enumeration so as to allow mapLimit.
for station in stations
	for year in years
		for month_obj in months
			month	= month_obj.month
			days	= month_obj.days
			for day in [1...days]
				_o = url.parse "http://climate.weather.gc.ca/climateData/bulkdata_e.html"
				delete _o['search']
				_query = { "stationID": station.id, "Year": year, "Month": month, "Day": day }
				for key,val of args
					_query[key] = val
				_o.query = _query
				_options.push _o

#_options = [ _options[0] ]

async.mapLimit _options, 100, ( _option_obj, cb ) ->
	log "Making request for #{_option_obj.query.stationID} #{_option_obj.query.Year}-#{_option_obj.query.Month}-#{_option_obj.query.Day}"
	req = http.request url.format( _option_obj ), ( res ) ->
		_r = ""

		res.setEncoding "utf8"

		res.on "data", ( chunk ) ->
			_r += chunk

		res.on "end", ( ) ->
			log "Got data for #{_option_obj.query.stationID} #{_option_obj.query.Year}-#{_option_obj.query.Month}-#{_option_obj.query.Day}"
			cb null, { stationID: _option_obj.query.stationID, data: parse_csv _r }

	req.on "error", ( err ) ->
		cb "Unable to get data for station '#{station.name}': #{err}"

	req.end( )
, ( err, res ) ->
	if err
		log err
		process.exit 1
	
	# Shove the docs into a couchdb database for now.
	#TODO.
	log "Finished grabbing data.."

parse_csv = ( data ) ->
	# Split out into a multi-dimensional array.

	_long_lines	= [ ]
	_longest	= 0
	for line in data.split "\n"
		_line = line.split ","

		# If there is a new longest line, reset the var..
		if _line.length > _longest
			_longest	= _line.length
			_long_lines	= [ ]

		# If the current line is the same length as the longest, push onto the array.
		if _line.length == _longest
			_long_lines.push _line

	_r	= { } 
	_keys	= [ ]

	keys	= _long_lines[0]

	# Iterate through and sanitze the keys.
	for key in keys

		# Strip out the quotes in the keys.
		key = key.replace /\"/g, ""

		# replace spaces and braces with underscored..
		key = key.replace /[\ \(\)]/g, "_"

		# Remove any unwanted characters.
		key = key.replace /[^a-zA-Z_]/g, ""

		# Replace any double underscores with a single one.
		key = key.replace /__/g, "_"

		# Trim any trailing underscores..
		key = key.replace /_$/, ""

		# Shove them into the valid keys array.
		_keys.push key

	# Create the array objects in the return..
	for _key in _keys
		_r[_key] = [ ]

	# Run through the value lines..
	for value_line in _long_lines[1..]

		# For each value line index
		for i in [0..value_line.length-1]

			# Sanitze the value..
			_value = value_line[i]

			# Remove the quotes..
			_value = _value.replace /\"/g, ""

			_r[_keys[i]].push _value
	return _r
