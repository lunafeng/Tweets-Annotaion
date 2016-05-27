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
		bestAnnotations = {}
		ambiSpots = [spot for spot in tweetSpots if tweetSpots[spot] == 1]
		disambiSpots = [spot for spot in tweetSpots if tweetSpots[spot] == 0]
		tweetVector = getTweetVector.main(tweet,tweetSpots)
		results =  getWikiConcepts.main(ambiSpots)
		wikiConcepts = results[0]
		for spot in disambiSpots:
			wikiConcepts[spot + "_0"] = "https://en.wikipedia.org/wiki/" + spot

		for sense in wikiConcepts:
			store = ""
			url = wikiConcepts[sense]	
			if url != None:
				titleList = wikiConcepts[sense].split("/wiki/")	
				print titleList[1]
				wikiVector = getWikiVectorNewSingle.main(titleList[1])
				result = docSim.main(tweet, wikiVector)
				senseList = sense.split("_")
				senseName = "_".join(senseList[:-1])
				senseId = senseList[-1]
				titleNew = str(titleList[1]).encode('string-escape')
				print titleNew
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
		for sense in wikiConcepts:	
			senseList = sense.split("_")
			senseName = "_".join(senseList[:-1])
			if senseName not in senseGot:	
				senseGot.append(senseName)
				cursor = db_mysql.cursor()
				sql = "SELECT Concept FROM TweetConceptSim Where Sense=\'" + senseName + "\' ORDER BY Sim DESC Limit 1"
				try:
					cursor.execute(sql)
					result = cursor.fetchone()
					concept = result[0]
					if concept not in Final:
						Final.append("https://en.wikipedia.org/wiki/" + concept)
				except:
					pass

		return Final

	

num = sys.argv[1]
basefilePath = "/data/CikmTwitterProject/WordsDisambiguation/words-disambiguation/Branches/b4/"
tweetspots = open(basefilePath + "Tagme_apathetic_v3_spot","r")
spots = open(basefilePath + "Spots_Tagme_apathetic_v3","r")
fd = open(basefilePath + "umbc_annotations_Tagme_apathetic_v3_STS0_nolimit", "a+")
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
