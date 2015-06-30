var redis = require("redis");
var express = require('express');
var async = require('async');
var ncdc = require('../ncdc');


var router = express.Router();




// Redis Client
client = redis.createClient();
client.on("error", function (err) {
	console.log("Redis Error: " + err);
});


// This callback will get the ZSET that is in key cacheKey,
// process those values, and send them back to the client.
// Call it once the autocomplete values are in the 
function finishAutocomplete(res, cacheKey)
{
	client.ZRANGE(cacheKey, 0, 10, function(err, values) {

		// Get the locations and their types in parallel.
		async.parallel([
			function(done) {
				client.HMGET("locations", values, function(err, locs) {
					done(null, locs)
				})
			},
			function(done) {
				client.HMGET("locationTypes", values, function(err, locTypes) {
					done(null, locTypes)
				})
			},
		],

		function(err, results) {
			// We've gotten the location names and types now.
			// Process them, and send them out to the client.
			var locs = results[0]
			var locTypes = results[1]

			// Give an empty array for no results.
			if (!locs)
				locs = []

			locs = locs.map(function(name, index) {
				if (!name) return null

				return {name: name, type: locTypes[index]}
			})

			// Remove nulls (can happen if locations are removed )
			locs = locs.filter(Boolean)

			res.json(locs)
		})
	})
}


// Autocomplete API endpoint.
// METHOD: GET
// PARAMETER: q
//     The search query to get autocomplete suggestions for
// RETURNS: JSON
//     An array with up to 10 autocomplete suggestions.
//     Each suggestion is of the form {name: "Washington", type: "State"}
//     Will be empty of there are no results.
router.get('/autoc', function(req, res, next) {
	var q = req.param("q")

	if (!q)
	{
		// Give an empty array for an empty query
		res.json([])
	}
	else
	{
		var words = ncdc.nameToWords(q)

		// The key that our ZSET will be stored in that contains
		// our autocomplete results.
		var cacheKey = "autoc:" + words.toString()

		client.EXISTS(cacheKey, function(err, exists) {
			if (exists)
			{
				// We've already computed the values. Send them back.
				finishAutocomplete(res, cacheKey)
			}
			else
			{
				// Compute the autocomplete results and store them temporarily

				// This syntax for ZINTERSTORE is from 
				// http://stackoverflow.com/questions/17087856/dynamic-arguments-for-zinterstore-with-node-redis
				var cmd = [ cacheKey, words.length ];
				var slocSets = words.map(function(word) { 
					return "sloc:" + word
				})

				client.ZINTERSTORE(cmd.concat(slocSets), function(err) {
					client.EXPIRE(cacheKey, 3600);

					finishAutocomplete(res, cacheKey)
				})
			}
		})


	}
});

module.exports = router;
