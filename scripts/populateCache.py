import requests

def populateCache():
	results = requests.get('localhost:10013?query=%7B%22groups%22%3A%5B%22date_year%22%5D%2C%22counttype%22%3A%22WordsPerMillion%22%2C%22words_collation%22%3A%22Case_Insensitive%22%2C%22database%22%3A%22Bookworm2021%22%2C%22search_limits%22%3A%5B%7B%22word%22%3A%5B%22polka%22%5D%2C%22date_year%22%3A%7B%22%24gte%22%3A1760%2C%22%24lte%22%3A2010%7D%2C%22languages__id%22%3A%5B%22122%22%5D%7D%5D%2C%22method%22%3A%22data%22%2C%22format%22%3A%22json%22%7D',headers={'Content-Type': 'application/json', 'Accept': 'application/json'})
	print(results)

populateCache()