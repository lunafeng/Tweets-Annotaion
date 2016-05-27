#!/usr/bin/python
import time
import docSim
import ast
import MySQLdb
from itertools import combinations
import re
import stemming
import getWikiVectorNew
import getWikiVectorNewSingle
import getWikiConcepts
import getTweetVector
import cosSim
import getConceptsSimilarity
import sys
import conceptSimilarity
db_mysql = MySQLdb.connect('141.117.3.92','lunafeng','luna222','WordsDisambiguation_b4')

def main(tweet,tweetSpots):
	db_mysql.ping()
	Final = []	
	ambiSpots = [spot for spot in tweetSpots if tweetSpots[spot] == 1]
	disambiSpots = [spot for spot in tweetSpots if tweetSpots[spot] == 0]
	tweetVector = getTweetVector.main(tweet,tweetSpots)
	results =  getWikiConcepts.main(ambiSpots)
	wikiConcepts = results[0]
	Concepts = []
	for spot in disambiSpots:
		wikiConcepts[spot + "_0"] = "https://en.wikipedia.org/wiki/" + spot

	for sense in wikiConcepts:
		store = ""
		url = wikiConcepts[sense]	
		if url != None:
			titleList = wikiConcepts[sense].split("/wiki/")	
			wikiVector = getWikiVectorNewSingle.main(titleList[1])
			result = docSim.main(tweet, wikiVector)
			senseList = sense.split("_")
			senseName = "_".join(senseList[:-1])
			senseId = senseList[-1]
			titleNew = str(titleList[1]).encode('string-escape')
			store += "(" + str(senseId) + ",\'" + str(senseName) + "\',\'" + titleNew + "\'," + str(result) + ")"
			sql = "INSERT IGNORE INTO TweetConceptSim VALUES " + store
			print sql
			try:
				cursor = db_mysql.cursor()
				cursor.execute(sql)
				db_mysql.commit()
			except:		
				db_mysql.rollback()
		
	senseGot = []	
	senseId_concept = {}
	for sense in wikiConcepts:	
		senseList = sense.split("_")
		senseName = "_".join(senseList[:-1])
		if senseName not in senseGot:	
			senseGot.append(senseName)
			cursor = db_mysql.cursor()
			sql = "SELECT Sense,SenseId,Concept,Sim FROM TweetConceptSim Where Sense=\'" + senseName + "\' ORDER BY Sim DESC Limit 2"
			try:
				cursor.execute(sql)
				results = cursor.fetchall()
				for r in results:
					senseId = str(r[0]) + "_" + str(r[1])
					senseId_concept[senseId] = (r[2], r[3])
			except:
				pass
	sql = "SELECT COUNT(DISTINCT(Sense)) FROM TweetConceptSim"
	cursor.execute(sql)
	result = cursor.fetchone()
	length = int(result[0])
	combinationsRaw = list(combinations(senseId_concept.keys(),length))
	spotsCombinations = []
	for combination in combinationsRaw:
		all = []
		goodCom = True
		for c in combination:
			spot = c[:-2]
			if spot not in all:
				all.append(spot)
			else:
				goodCom = False
				break
		if goodCom is True:
			spotsCombinations.append(combination)
	max = 0
	comMax = []
	print len(spotsCombinations)
	sys.stdout.flush()
	for com in spotsCombinations:
		print com
		result = 1
		pairwise = list(combinations(list(com), 2))
		for c in com:
			if float(senseId_concept[c][1]) != 0:
				result *= float(senseId_concept[c][1])
			else:
				result *= float(0.0001)
		for pair in pairwise:
			wikiVector1 = getWikiVectorNewSingle.main(senseId_concept[pair[0]][0])
			wikiVector2 = getWikiVectorNewSingle.main(senseId_concept[pair[1]][0])
			sim = docSim.main(wikiVector1, wikiVector2)
			if sim == float(0):
				sim = 0.0001
			result *= sim
		if result > max:
			max = result
			comMax = com
		print result
	
	for c in comMax:
		concept = senseId_concept[c][0]
		if concept not in Final:
			Final.append("https://en.wikipedia.org/wiki/" + concept)
	return Final	
		


num = sys.argv[1]
basefilePath = "/data/CikmTwitterProject/WordsDisambiguation/words-disambiguation/Branches/b4/"
tweetspots = open(basefilePath + "Tagme_apathetic_v3_spot","r")
spots = open(basefilePath + "Spots_Tagme_apathetic_v3","r")
fd = open(basefilePath + "umbc_annotations_Tagme_apathetic_v4_STS0_nolimit", "a+")
spotsHash = {}
spotscontents = spots.readlines() 
for spot in spotscontents:
	spot = spot.strip("\n")
	spotList = spot.split("\t")
	spotsHash[spotList[0]] = int(spotList[1])
contents = tweetspots.readlines()

for i in range(len(contents) - int(num)):
	c = contents[i + int(num)]
	tweetSpots = {}
	c = c.strip("\n")
	cList = c.split("\t")
	tweet = cList[1]
	spots = ast.literal_eval(cList[2])
	for spot in spots:
		spot = spot.lower()
		spot = spot.replace(" ", "_")
		tweetSpots[spot] = spotsHash[spot]
	print tweet
	print tweetSpots
	annotations = main(tweet,tweetSpots)
	fd.write(cList[0] + "\t" + tweet + "\t" + str(annotations) + "\n")
	fd.flush()
	db_mysql.ping()
	cursor = db_mysql.cursor()
	sql = "TRUNCATE TweetConceptSim"
	cursor.execute(sql)
	db_mysql.commit()
