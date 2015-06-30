var async = require('async');
var request = require('request');
var express = require('express');
var redis = require("redis");
var RateLimiter = require('limiter').RateLimiter;

var apiKey = require("./key").apiKey




// Redis Client
client = redis.createClient();
client.on("error", function (err) {
	console.log("Redis Error: " + err);
});





// This limiter is very unreliable.
// It only serves to throttle the bulk of requests,
// but we still have to watch for those that NCDC give a 429 for.
var limiterSec = new RateLimiter(4, "sec");


// Perform an outgoing request to the NCDC api.
// @param {object} options - 
//     An options table for the request library.
// @param {function} callback - 
// 	    A callback to be called when the request completes successfully.
//      Called as callback(response, body)
// @param {function} errorCallback
//     A callback to be called when the request fails for any reason.
function apiOut(options, callback, errorCallback) {
	limiterSec.removeTokens(1, function(err, remainingRequests) {

		options["headers"] = {
			"token": apiKey,
		}
		request(options, function(error, response, body) {

			if (error)
				return errorCallback(error)

			var json = JSON.parse(body)

			// If we hit the limit, try again.
			if (json["status"] == "429")
			{
				if (json["message"].indexOf("5 per second") >= 0)
				{
					console.log("temp limit reached - retrying")
					return apiOut(options, callback, errorCallback)
				}
				else if (errorCallback)
				{
					console.log(json)
					return errorCallback("The server has reached its maximum number of requests for the day.")
				}
			}

			callback(response, body)
		})
	})
}
module.exports.apiOut = apiOut




function harvestError(err)
{
	console.log("error while harvesting: " + err)
}


// Query the API for the number of locations in the given location category.
// Calls callback with that number as its only parameter when complete.
function getNumLocations(locationcategoryid, callback)
{
	apiOut(
		{
			url: "http://www.ncdc.noaa.gov/cdo-web/api/v2/locations",
			qs: {
				limit: 1,
				locationcategoryid: locationcategoryid,
				datacategoryid: "TEMP",
			},
		},
		function(response, body) {
			var count = JSON.parse(body)["metadata"]["resultset"]["count"]

			callback(count)
		},
		harvestError
	)
}


// Maps ASCII character codes to a constant index, starting at 1.
// Only maps lowercase letters and numbers. All other values will be more-or-less
// ignored by calcScore().
var charValueMap = {};

// Build charValueMap
(function(){
	var mapIdx = 1
	for (var i = ("0").charCodeAt(0); i <= ("9").charCodeAt(0); i++)
	{
		charValueMap[i] = mapIdx;
		mapIdx += 1
	}
	for (var i = ("a").charCodeAt(0); i <= ("z").charCodeAt(0); i++)
	{
		charValueMap[i] = mapIdx;
		mapIdx += 1
	}
})();



// Calculate the sorting score for a given string.
// Baseline is a value that will be the most dominant factor in the score.
// Lower values give lower outputs. Lower outputs will occur first in search results.
var scoreMaxSize = 30
function calcScore(key, baseline)
{
	var score = 0

	score += baseline*(Math.pow(27, scoreMaxSize+1))

	for (var i = 0; i < Math.min(key.length, scoreMaxSize); i++) {
		
		var c = charValueMap[key.charCodeAt(i)]
		if (!c) c = 1

		score += c*(Math.pow(27, scoreMaxSize-i))
	};
	return score
}
module.exports.calcScore = calcScore


// Given a string, returns an array of all the words
// (consecutive characters), in lower case.
function nameToWords(name)
{
	return name
		.trim()
		.toLowerCase()
		.replace(",", "") // no commas
		.replace(/\s{2,}/g,' ') // no duplicate spaces
		.split(" ")
}
module.exports.nameToWords = nameToWords


// Harvest locations for the given location category at the specified offset.
function harvestLocations(locationcategoryid, offset, baselineScore)
{
	apiOut(
		{
			url: "http://www.ncdc.noaa.gov/cdo-web/api/v2/locations",
			qs: {
				limit: 1000,
				offset: offset,
				locationcategoryid: locationcategoryid,
				datacategoryid: "TEMP",
			},
		},
		function(response, body) {
			console.log(locationcategoryid + ": Got offs " + offset)
			var json = JSON.parse(body)

			locs = json["results"]

			if (!locs)
			{
				console.log("didn't get locs: " + body)
				return
			}

			for (var i = 0; i < locs.length; i++) {
				var loc = locs[i]

				var id = loc["id"]
				var key = "loc:" + id
				var name = loc["name"]

				// Store the information about the location.
				client.HMSET(key, loc)

				// Expire it after two days
				client.EXPIRE(key, 60*60*24*2)
				
				// Add the location's human-readable name to a hash of all locations,
				// keyed by its ID.
				client.HSET("locations", id, name)

				name = name.toLowerCase()
				var score = calcScore(name, baselineScore)
				var words = nameToWords(name)
				
				// For each word in the human-readable name,
				// take all the substrings that start at the beginning of the word,
				// and add the location's ID to a sorted set keyed by that substring.
				for (var w = 0; w < words.length; w++) {
					var word = words[w]

					for (var c = 1; c <= word.length; c++)
					{
						client.ZADD("sloc:" + word.substring(0, c), score, id)
					}
				}
			};
		},
		harvestError
	)
}

// Begin harvesting location data from NCDC for the given category.
// baselineScore is a value that will be the most dominant factor in determining
// the sorted order of the results. Lower values give higher priority.
function harvestLocationCategory(locationcategoryid, baselineScore)
{
	console.log("Starting location harvest: " + locationcategoryid)
	getNumLocations(locationcategoryid, function(count) {
		console.log(locationcategoryid + ": Got " + count + " locations")
		for (var i = 1; i < count; i += 1000)
		{
			harvestLocations(locationcategoryid, i, baselineScore)
		}
	})
}



// Begin harvesting location data from NCDC.
// Initiates harvests for CNTRY, ST, CITY, CNTY, and ZIP
function startLocationHarvest()
{
	client.HKEYS("locations", function (err, keys) {
		
		console.log("Pruning old locations")
		async.each(keys, function(key, callback) {
			client.EXISTS("loc:" + key, function(err, exists) {
				if (!exists)
				{
					client.HDEL("locations", key)
					console.log("Pruned location " + key)
				}
				callback()
			})
		},
		function() {
			// A low baseline score (param2) gives higher position in searches
			harvestLocationCategory("ZIP", 5)
			harvestLocationCategory("CNTY", 4)
			harvestLocationCategory("CITY", 3)
			harvestLocationCategory("ST", 2)
			harvestLocationCategory("CNTRY", 1)
		})
	})

	setTimeout(startLocationHarvest, 1000*60*60*12) // every 12 hours.
}


// uncomment if an immediate harvest is needed due to changes in the code.
// AND FOR THE LOVE OF GOD, COMMENT OUT THE OTHER LINE
// IN CASE THIS MAKES IT INTO PRODUCTION

//startLocationHarvest();

// do it in an hour. Prevents excessive harvests during development.
setTimeout(startLocationHarvest, 1000*60*60)

