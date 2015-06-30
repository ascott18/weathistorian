var app = angular.module('wh', ['ui.bootstrap'])

app.controller('locSearch', function($scope, $http) {

	$scope.selected = undefined;

	// Any function returning a promise object can be used to load values asynchronously
	$scope.getLocation = function(val) {
		return $http.get('/api/autoc', {
			params: {
				q: val
			}
		}).then(function(response){
			return response.data
		});
	};

});