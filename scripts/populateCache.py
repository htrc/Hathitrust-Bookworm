import requests, json

def populateCache():
	for lang in ['1','2']:
		params = {"groups":["date_year"],"counttype":"WordsPerMillion","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010},"languages__id":[lang]}],"method":"data","format":"json"}
		json_string = json.dumps(params)
		url = 'http://localhost:10013?query=' + json_string
		print(url)
		results = requests.get(url,headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
		print(results)
		print(results.content)

populateCache()