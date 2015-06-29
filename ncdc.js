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





var limiterSec = new RateLimiter(4, "sec");

function apiOut(options, callback, errorCallback) {
	limiterSec.removeTokens(1, function(err, remainingRequests) {
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
					return errorCallback("The server has reached its maximum number of requests for the day.")
				}
			}

			callback(error, response, body)
		})
	})
}




// Location harvesting

function harvestError(err)
{
	console.log("error while harvesting: " + err)
}

function getNumLocations(locationcategoryid, callback)
{
	apiOut(
		{
			url: "http://www.ncdc.noaa.gov/cdo-web/api/v2/locations",
			headers: {
				"token": apiKey,
			},
			qs: {
				limit: 1,
				locationcategoryid: locationcategoryid,
				datacategoryid: "TEMP",
			},
		},
		function(error, response, body) {
			var count = JSON.parse(body)["metadata"]["resultset"]["count"]

			callback(count)
		},
		harvestError
	)
}

function harvestLocations(locationcategoryid, offset)
{
	apiOut(
		{
			url: "http://www.ncdc.noaa.gov/cdo-web/api/v2/locations",
			headers: {
				"token": apiKey,
			},
			qs: {
				limit: 1000,
				offset: offset,
				locationcategoryid: locationcategoryid,
				datacategoryid: "TEMP",
			},
		},
		function(error, response, body) {
			console.log(locationcategoryid + ": Got offs " + offset)
			var json = JSON.parse(body)

			locs = json["results"]

			if (!locs)
				console.log("didn't get locs: " + body)

			for (var i = 0; i < locs.length; i++) {
				var loc = locs[i]

				var key = "loc:" + loc["id"]
				client.hmset(key, loc)
				client.expire(key, 60*60*24*2) // 2 days
				client.hset("locations", loc["id"], loc["name"])
			};
		},
		harvestError
	)
}

function harvestLocationCategory(locationcategoryid)
{
	console.log("Starting location harvest: " + locationcategoryid)
	getNumLocations(locationcategoryid, function(count) {
		console.log(locationcategoryid + ": Got " + count + " locations")
		for (var i = 1; i < count; i += 1000)
		{
			harvestLocations(locationcategoryid, i)
		}
	})
}




var router = express.Router();
router.get('/', function(req, res, next) {
	client.hgetall("locations", function (err, obj) {
	    res.send(obj)
	})
});


function startLocationHarvest()
{
	client.hkeys("locations", function (err, keys) {
		
		console.log("Pruning old locations")
		async.each(keys, function(key, callback) {
			client.exists("loc:" + key, function(err, exists) {
				if (!exists)
				{
					client.hdel("locations", key)
					console.log("Pruned location " + key)
				}
				callback()
			})
		},
		function() {
			harvestLocationCategory("CITY")
			harvestLocationCategory("CNTY")
			harvestLocationCategory("ST")
			harvestLocationCategory("ZIP")
			harvestLocationCategory("CNTRY")
		})

	})

	setTimeout(startLocationHarvest, 1000*60*60*12) // every 12 hours.
}

startLocationHarvest();

module.exports = router;
