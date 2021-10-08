import pandas as pd
import dask.dataframe as dd
import os, sys
import re as regex
import numpy as np
from htrc_features import FeatureReader, utils

def testAgainstDictionaries(features,data,final):
	# Two copies of the same dictionary, Laird and Lee's Webster's. They capitalize their words, so
	# I'm looking for capital words that occur in both.
	dicts = ['loc.ark:/13960/t84j1sb5j', 'loc.ark:/13960/t3xs70k06']
	paths = [features + utils.id_to_rsync(volid) for volid in dicts]
	fr = FeatureReader(paths)
	tokenlist = []
	for vol in fr.volumes():
	    tokenlist += vol.tokens()

	tokens = pd.Series(tokenlist)
	# Grab capitalized letters
	dictionary_words = tokens[tokens.str.contains(r"^[A-Z][A-Z\-]*$")].value_counts()
	shortlist = dictionary_words[dictionary_words > 1].index.str.lower().values

	unique_final_lower = final['token'].str.lower().unique()
	extradictwords = np.setdiff1d(shortlist, unique_final_lower)
	print(pd.Series(extradictwords).sample(50))
	print(shortlist.shape[0], extradictwords.shape[0], 1-extradictwords.shape[0]/shortlist.shape[0])
#	df = pd.read_hdf(data + 'final/wordlist.h5')
#	df.to_csv(data + 'final/wordlist.csv',sep='\t',index=False)

def addProblemCharactersToWordlist(final_candidate,prob_chars):
	# Problem characters that are not in the wordlist: add them (as fixed version)
	to_add = np.setdiff1d(prob_chars['token'].values, final_candidate['token'].values)
	new_lines = prob_chars[prob_chars['token'].isin(to_add)][['token', 'count']]
	final = pd.concat([final_candidate, new_lines])\
			.groupby('token', as_index=False).sum()\
			.sort_values('count', ascending=False)\
			.reset_index(drop=True)\
			.reset_index()

	return final

def testJunkRemovalRules(wordlist):
	print(wordlist.shape)
	tokens = wordlist.index
	hyphenated = tokens.str.contains(r"-")
	#alpha = tokens.str.isalpha() # Faster, but bad for some languages
	alphaadv = tokens.map(lambda x: not not regex.search("^\\w+$", x)).values.astype(np.bool_)
	number = tokens.str.contains(r"^(£|$|€)?[\d.,]+(st|nd|rd|th|s|C|F|c|m|°|¥)?$")
	singlequote = tokens.str.contains(r"[\'’]")
	abbr = tokens.str.contains(r"^[^\W\d]([^\W\d]|\.)+$")
	endwithperiod = tokens.str.endswith('.')
	# This shows up for many asian characters, should be dealt with *before* wordlist is created
	blankchar = (tokens.str.startswith('\u200b') | tokens.str.endswith("\u200b"))
	tlen = tokens.str.len()
	print(len(tokens))

	print(len(tokens))
	print(type(hyphenated))
	print(type(hyphenated[0]))
	print(~hyphenated)
	print(type(alphaadv))
	print(type(alphaadv[0]))
	print(~alphaadv)
	print(tlen >= 2)
	print(~endwithperiod)
	print(~singlequote)
	print(~number)
	print(~abbr)
	print(~blankchar)
	print(wordlist[~hyphenated & ~alphaadv & (tlen >= 2) & ~endwithperiod & ~singlequote & ~number & ~abbr & ~blankchar].index.values[:100])

	print(len(wordlist))
	print(len(~hyphenated))
	print(len(~alphaadv))
	print(len(tlen >= 2))
	print(len(~endwithperiod))
	print(len(~singlequote))
	print(len(~number))
	print(len(~abbr))
	print(len(~blankchar))
	junk = wordlist[~hyphenated & ~alphaadv & (tlen >= 2) & ~endwithperiod & ~singlequote & ~number & ~abbr & ~blankchar]
	print(junk.head(10))
	print(junk.sample(10))

	final_candidate = wordlist[hyphenated | alphaadv | (tlen < 2) | endwithperiod | singlequote | number | abbr | blankchar].reset_index()
	return final_candidate

def testTrimPolicy(data,start=9*10**5):
	# Grab a 1000 word chunk starting at 'start'
	lang = '/eng'
	test_tokens = pd.read_hdf(data + 'final/final-sorted.h5', lang, start=start, stop=start+1000)
	print(test_tokens.sample(10))

def findProblemDFS(cutoff_list,dfs,problem_dfs,data):
	for i, row in cutoff_list.iterrows():
		if row['retain_count'] == 0:
			continue
		df = pd.read_hdf(data + 'final/final-sorted.h5', row['lang'], stop=row['retain_count'])
		# Save Japanese and Chinese chars with \u200b char
		if row['lang'] in ['/jpn', '/chi', '/kor', '/arm', '/urd']:
			problems = df[(df.index.str.startswith('\u200b') | df.index.str.endswith("\u200b"))]
			problem_dfs.append(problems)
		dfs.append(df)

def trim_topwords(row):
	top_words_ref = dict(eng=900000, ger=650000, fre=400000, lat=360000, rus=280000, jpn=300000, ita=220000, spa=220000)

	if row[0][1:] in top_words_ref:
		return top_words_ref[row[0][1:]]
	elif row[1] < 100000:
		# Ignore langs with practically no words as likely duds, or at the very least
		# something BW wouldn't be useful for
		return 0
	else:
		# Other languages: keep greater of 25k or 5% of vocab
		mincount = 20000
		percentagetrim = int(row[1] * 0.035)
		return percentagetrim if percentagetrim > mincount else mincount

def createWordlist(features,data,core_count):
	st = os.stat(data + 'final/final-sorted.h5')
	print(st.st_size / 1024**3)

	# Get all langs and their sizes
	with pd.HDFStore(data + 'final/final-sorted.h5') as store:
		keys = store.keys()
		sizes = [store.get_storer(key).nrows for key in keys]
	tablesizes = pd.Series(sizes, index=keys).sort_values(ascending=False)
	print(tablesizes.head(5))

	cutoff_list = tablesizes.reset_index().rename(columns={'index': 'lang', 0: 'count'})
	cutoff_list['retain_count'] = cutoff_list.apply(trim_topwords, axis=1)
	print("Total tokens (including possible dupes)", cutoff_list['retain_count'].sum())
	print(cutoff_list.head(10))

	dfs = []
	problem_dfs = []
	findProblemDFS(cutoff_list,dfs,problem_dfs,data)
	asn_probchars = pd.concat(problem_dfs).groupby(level='token').sum().sort_values('count', ascending=False)
	print(asn_probchars)
	wordlist = pd.concat(dfs).groupby(level='token').sum().sort_values('count', ascending=False)
	print("Final wordlist using top N trim criteria: ", wordlist.shape)

	testTrimPolicy(data,start=0)

	final_candidate = testJunkRemovalRules(wordlist)
#	testJunkRemovalRules(pd.read_hdf(data + 'final/final-sorted.h5', 'hin', stop=1000000).head())

	prob_chars = asn_probchars.reset_index().query('token != "\u200b"')
	print(prob_chars)
	prob_chars['broken'] =prob_chars['token']
	prob_chars['token'] = prob_chars['broken'].str.replace('\u200b', '')

	final = addProblemCharactersToWordlist(final_candidate,prob_chars)

	# The indices to for the fixed characters. When we encounter the broken words in the dataset, we'll encode then
	# with the id for the fixed token
	print(len(final))
	print(len(prob_chars[['token','broken']]))
	problemchar_indices = pd.merge(final, prob_chars[['token', 'broken']], on='token')[['index','broken']]
	if len(prob_chars[['token','broken']]) > 0:
		problemchar_indices.sample(3)

	# OVERWRITE MODE
	with pd.HDFStore(data + 'final/wordlist.h5', complib='blosc', mode='w', complevel=9) as store:
		try:
			store.append('/final', final)
		except Exception as e:
			print(e)

		try:
			store.append('/fixes', problemchar_indices)
		except Exception as e:
			print("b")
			print(e)

	df = pd.read_hdf(data + 'final/wordlist.h5')
	df.to_csv(data + 'final/wordlist.csv',sep='\t',index=False)
#	testAgainstDictionaries(features,data,final)

if __name__ == "__main__":
	createWordlist(sys.argv[1],sys.argv[2],sys.argv[3])