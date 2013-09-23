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

_options = [ _options[0] ]

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
	log res

parse_csv = ( csv_data ) ->
	_long_lines	= [ ]
	_longest	= 0
	for line in csv_data.split "\n"
		_line = line.split ","

		# If there is a new longest line, reset the var..
		if _line.length > _longest
			_longest	= _line.length
			_long_lines	= [ ]

		# If the current line is the same length as the longest, push onto the array.
		if _line.length == _longest
			# Strip out the bad values of the keys and values..
			_long_lines.push _line
	
	# Iterate through the keys and create the
	# keys array.
	keys = [ ]
	for _key in _long_lines[0]

		# Replace any characters that we don't either
		# want to replace or that don't belog with nothing..
		_key = _key.replace /[^a-zA-Z_\/ ]/g, ""

		# Replace slashes and spaces with underscores.
		_key = _key.replace /[\/\ ]/g, "_"

		# Lowercase the entire thing.
		_key = _key.toLowerCase( )

		keys.push _key

	_return = [ ]

	# Iterate over the actual line values..
	for _line in _long_lines[1..]
		
		# The object that we're going to populate with valid values..
		_r = { }

		# Iterate over each value..
		for i in [0.._line.length-1]

			# Sanitize the value..
			_val = _line[i]
			_val = _val.replace /[^0-9\.\-]/g, ""
			
			# Try and parse it as a float.
			_float	= parseFloat _val

			# If we can't parse it as a float, simply set the val to be whatever
			# we already have.
			if isNaN _float
				_r[keys[i]] = _val
			else
				_r[keys[i]] = _float

		_return.push _r

	return _return
