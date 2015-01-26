"""
	Clase para el manejo de la base de datos 

	Autor: Victor R Martinez
	Fecha (ult mod): 9 ene 15 
	Version: 3.0
"""

import MySQLdb
import MySQLdb.cursors

class DatabaseManager:

	def __init__(self, host, user, passwd, db):
		self.__err = ""
		self.isConnectionAlive = False
		self.db = None
		self.__query = ""
		self.db_args = {"host":host, "user":user, "passwd":passwd, "db":db}

	def getConnection(self):
		try:
			if not self.isConnectionAlive:
				self.db = MySQLdb.connect(host=self.db_args['host'],
										  user=self.db_args['user'],
										  passwd=self.db_args['passwd'],
										  db=self.db_args['db'],
										  cursorclass=MySQLdb.cursors.DictCursor)
				self.isConnectionAlive = True	
			return self.db
		except Exception, e:
			self.__err = e.message
			return None
		
	def getError(self):
		return self.__err

	def queryCursor(self, query):
		"""Ejecuta la query deseada y devuelve una lista de resultados"""	

		# print("QUERY: %s" % query)

		self.__query = query
		cursor = self.getConnection().cursor()
		cursor.execute(query)
		return cursor

	def runQuery_fetchOne(self, query):
		return self.queryCursor(query).fetchone()
		
	def runQuery_fetchAll(self, query):
		try:
			return self.queryCursor(query).fetchall()
		except:
			return None

	def runQuery(self, query):
		return runQuery_fetchAll()

	def runCommit(self, query):
		"""Ejecuta la query deseada con commit"""
		try:
			self.__query = query
			cursor = self.getConnection().cursor()
			cursor.execute(query)
			self.db.commit()
			return True
		except Exception, e:
			# print "Query: " + self.__query
			# print "ERR: " + e.message
			self.__err = e.message
			self.db.rollback()
			return False

	def __del__(self):
		if self.db:
			self.db.close()
		self.isConnectionAlive = None

