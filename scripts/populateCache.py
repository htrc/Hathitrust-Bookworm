import requests, urllib, json

def populateCache():
	params = {"groups":["date_year"],"counttype":"WordsPerMillion","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010},"languages__id":["122"]}],"method":"data","format":"json"}
	json_string = json.dumps(params)
	print(json_string)
	encoded_params = urllib.parse.quote_plus(json_string)
	print(encoded_params)
	url = 'http://localhost:10013?query=' + encoded_params
	print(url)
	results = requests.get(url,headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
	print(results)
	print(results.content)

populateCache()