"""
	Clase para el manejo de la tabla de Tweets 

	Autor: Victor R Martinez
	Fecha (ult mod): 9 ene 15 
	Version: 3.0
"""
from User import User
from string import Template 

class Tweet(object):

	INSERT_QUERY = Template(open('templates/insertTweet.template').read())
	SEARCH_QUERY = Template(open('templates/searchTweetById.template').read())
	SELECT_QUERY = Template(open('templates/selectTweet.template').read())



	def __init__(self,
				 databaseMngr = None):
		self.tweetID = "NULL"
		self.tweet_id_str = "" 
		self.from_user = ""
		self.tweet = ""
		self.created_at = ""
		self.source = ""
		self.favorite_count = 0
		self.retweet_count = 0
		self.coordinates = ""
		self.lang = ""
		self.filter_level = ""
		self.in_reply_to_status_id_str = ""
		self.dbm = databaseMngr
		
	def set(self, 
			tweetID = "NULL",
			tweet_id_str = "",
			from_user = "",
			tweet = "",
			created_at = "",
			source = "",
			favorite_count = 0,
			retweet_count = 0,
			coordinates = "",
			lang = "",
			filter_level = "",
			in_reply_to_status_id_str = ""):
		self.tweetID = tweetID
		self.tweet_id_str = tweet_id_str
		self.from_user = from_user
		self.tweet = tweet
		self.created_at = created_at
		self.source = source
		self.favorite_count = favorite_count
		self.retweet_count = retweet_count
		self.coordinates = coordinates
		self.lang = lang
		self.filter_level = filter_level
		self.in_reply_to_status_id_str = in_reply_to_status_id_str

	def save(self):
		"""Inserta un nuevo Tweet en la base de datos"""
		if self.dbm:
			query = Tweet.INSERT_QUERY.safe_substitute(self.__dict__)
			# print(query)
			return self.dbm.runCommit(query)
		else:
			raise Exception("No dbm declared")

	@staticmethod
	def searchTweetById(dbm, tweetID):
		"""Regresa un objeto Tweet para la id buscada"""
		tweetRes = Tweet(dbm)
		if dbm is not None:
			row = dbm.runQuery_fetchOne(Tweet.SEARCH_QUERY.substitute({'tweetID':tweetID}))
			if row is not None:

				tweetRes.set(	tweetID = row["tweetID"],
							 	tweet_id_str = row["tweet_id_str"],
							 	from_user = row["from_user_id"],
								tweet = row["tweet"],
								created_at = row["created_at"],
								source = row["source"],
								favorite_count = row["favorite_count"],
								retweet_count = row["retweet_count"],
								coordinates = row["coordinates"],
								lang = row["lang"],
								filter_level = row["filter_level"],
								in_reply_to_status_id_str = row["in_reply_to_status_id_str"])
			return tweetRes
		else:
			raise Exception("No DBM declared")

	@staticmethod
	def getAllTweets(dbm):
		"""Regresa una lista de objetos Tweet con todos los elementos de la tabla"""
		allTweets = []
		if dbm is not None:
			res = dbm.runQuery_fetchAll(Tweet.SELECT_QUERY)
			for row in res:
				tweetRes = Tweet()
				tweetRes.set(	tweetID = row["tweetID"],
								tweet_id_str = row["tweet_id_str"],
								from_user = row["from_user_id"],
								tweet = row["tweet"],
								created_at = row["created_at"],
								source = row["source"],
								favorite_count = row["favorite_count"],
								retweet_count = row["retweet_count"],
								coordinates = row["coordinates"],
								lang = row["lang"],
								filter_level = row["filter_level"],
								in_reply_to_status_id_str = row["in_reply_to_status_id_str"])
				tweetRes.dbm = dbm
				allTweets.append(tweetRes)
			return allTweets
		else:
			raise Exception("No DBM declared")
