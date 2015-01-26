class User:
	"""Objeto que representa una entrada en la tabla Users"""
	def __init__(self, databaseMngr = None):
		self.idUser = 0
		self.created_at = ""
		self.description = ""
		self.followers_count = 0
		self.friends_count = 0
		self.geo_enabled = 0
		self.location = ""
		self.name = ""
		self.protected = 0
		self.time_zone = ""
		self.url = ""
		self.verified = 0
		self.dbm = databaseMngr

	def set(self, idUser, created_at = "", description = "", followers_count = 0, friends_count = 0, geo_enabled = False, location = "", name = "", protected = False, time_zone = "", url = "", verified = False):
		"""Establece los campos del objeto User"""
		self.idUser = idUser
		self.created_at = created_at
		self.description = description
		self.followers_count = followers_count
		self.friends_count = friends_count
		self.location = location
		self.name = name
		self.time_zone = time_zone
		self.url = url

		if isinstance(protected, bool):
			self.protected = 1 if protected else 0
		elif isinstance(protected, int):
			self.protected = 1 if (protected > 0) else 0

		if isinstance(geo_enabled, bool):
			self.geo_enabled = 1 if geo_enabled else 0
		elif isinstance(geo_enabled, int):
			self.geo_enabled = 1 if (geo_enabled > 0) else 0

		if isinstance(verified, bool):
			self.verified = 1 if verified else 0
		elif isinstance(verified, int):
			self.verified = 1 if (verified > 0) else 0		

	def saveOrUpdate(self):
		"""Actualiza o inserta un nuevo User en la base de datos"""
		if self.dbm:
			query = u"INSERT INTO Users (id, created_at, description, followers_count, friends_count, geo_enabled, location, name, protected, time_zone, url, verified) VALUES ({0}, \"{1}\", \"{2}\", {3}, {4}, {5}, \"{6}\", \"{7}\", {8}, \"{9}\", \"{10}\", {11}) ON DUPLICATE KEY UPDATE created_at=VALUES(created_at), description=VALUES(description), followers_count=VALUES(followers_count), friends_count=VALUES(friends_count), geo_enabled=VALUES(geo_enabled), location=VALUES(location), name=VALUES(name), protected=VALUES(protected), time_zone=VALUES(time_zone), url=VALUES(url), verified=VALUES(verified)".format(self.idUser, self.created_at, self.description, self.followers_count, self.friends_count, self.geo_enabled, self.location, self.name, self.protected, self.time_zone, self.url, self.verified)
			self.dbm.runCommit(query)
		else:
			raise Exception("No DBM declared")

	@staticmethod
	def searchUserById(dbm, idUser):
		"""Regresa un objeto User para la id buscada"""
		userRes = User(dbm)
		if dbm is not None:
			try:
				row = dbm.runQuery_fetchOne("SELECT id, created_at, description, followers_count, friends_count, geo_enabled, location, name, protected, time_zone, url, verified FROM Users WHERE id = {}".format(idUser))
				if row is not None:
					userRes.set( idUser = row["id"],
								 created_at = row["created_at"],
								 description = row["description"],
								 followers_count = row["followers_count"],
								 friends_count = row["friends_count"],
								 geo_enabled = row["geo_enabled"],
								 location = row["location"],
								 name = row["name"],
								 protected = row["protected"],
								 time_zone = row["time_zone"],
								 url = row["url"],
								 verified = row["verified"])
			except:
				pass
			return userRes
		else:
			raise Exception("No DBM declared")

	@staticmethod
	def getAllUsers(dbm):
		"""Regresa una lista de objetos User con todos los elementos de la tabla"""
		allUsers = []
		if dbm is not None:
			res = dbm.runQuery("SELECT id, created_at, description, followers_count, friends_count, geo_enabled, location, name, protected, time_zone, url, verified FROM Users")
			for row in res:
				userRes = User(dbm)
				userRes.set( idUser = row["id"],
							 created_at = row["created_at"],
							 description = row["description"],
							 followers_count = row["followers_count"],
							 friends_count = row["friends_count"],
							 geo_enabled = row["geo_enabled"],
							 location = row["location"],
							 name = row["name"],
							 protected = row["protected"],
							 time_zone = row["time_zone"],
							 url = row["url"],
							 verified = row["verified"])
				userRes.dbm = dbm
				allUsers.append(userRes)
			return allUsers
		else:
			raise Exception("No DBM declared")

	def __str__(self):
		return "User<(%i)>".format(self.idUser)