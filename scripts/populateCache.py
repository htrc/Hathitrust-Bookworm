import requests, json

def sendGetRequest(params):
	json_string = json.dumps(params)
	url = 'http://localhost:10013?query=' + json_string
	print(url)
	results = requests.get(url,headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
	print(results)
	print(results.content)

def processRequest(params):
	sendGetRequest(params)
	params['groups'] = []
	params['counttype'] = ["TextCount","WordCount"]
	params['search_limits'][0].pop('word',None)
	sendGetRequest(params)

def loadLineGraph():
	processRequest({"groups":["date_year"],"counttype":"WordsPerMillion","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010}}],"method":"data","format":"json"})
	processRequest({"groups":["date_year"],"counttype":"WordsPerMillion","words_collation":"Case_Sensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010}}],"method":"data","format":"json"})
	processRequest({"groups":["date_year"],"counttype":"TextPercent","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010}}],"method":"data","format":"json"})
	processRequest({"groups":["date_year"],"counttype":"WordCount","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010}}],"method":"data","format":"json"})

	for lang in range(1,199):
		params = {"groups":["date_year"],"counttype":"WordsPerMillion","words_collation":"Case_Insensitive","database":"Bookworm2021","search_limits":[{"word":["polka"],"date_year":{"$gte":1760,"$lte":2010},"languages__id":[str(lang)]}],"method":"data","format":"json"}
		processRequest(params)

def loadMap():
	sendGetRequest({"search_limits":{"word":["color"]},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publication_country"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["colour"]},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publication_country"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["color"],"publication_country":"United States"},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["*publication_country","publication_state"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["colour"],"publication_country":"United States"},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["*publication_country","publication_state"],"database":"Bookworm2021"})

def loadHeatmap():
	sendGetRequest({"search_limits":{"word":["computer"],"lc_classes__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["lc_classes","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"lc_subclass__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["lc_subclass","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"fiction_nonfiction__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["fiction_nonfiction","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"genres__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["genres","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"languages__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["languages","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"htsource__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["htsource","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"digitization_agent_code__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["digitization_agent_code","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"mainauthor__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["mainauthor","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"publisher__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publisher","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"format__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["format","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"page_count_bin__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["page_count_bin","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"word_count_bin__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["word_count_bin","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"publication_country__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publication_country","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"publication_state__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publication_state","date_year"],"database":"Bookworm2021"})
	sendGetRequest({"search_limits":{"word":["computer"],"publication_place__id":{"$lt":31},"date_year":{"$lt":2015,"$gt":1650}},"words_collation":"case_insensitive","method":"data","format":"json","counttype":["WordsPerMillion"],"groups":["publication_place","date_year"],"database":"Bookworm2021"})

def populateCache():
	loadLineGraph()
	loadMap()
	loadHeatmap()

populateCache()