import mysql.connector
from config_functions import read_config

#connection = None
class dboperator:
	def __init__(self):
		self.connection = None
		self.cursor = None

	# подключиться к базе данных
	def connect(self):
		mysqlConfigPart = read_config('mysql')
		self.connection = mysql.connector.connect(**mysqlConfigPart)
		if self.connection.is_connected():	
			self.cursor = self.connection.cursor()
		else:
			raise Exception('не удалось подключиться к базе данных')

	# подставить значение переменных в sql запрос
	def prepareQuery(self, query, args):
		arguments = {}
		for arg in args:
			a = type(args[arg]).__name__
			if a == 'date' or a == 'datetime' or a == 'str':
				arguments[arg] = '\'' + str(args[arg]) + '\''
			else:
				arguments[arg] = str(args[arg])
			find = "$" + str(arg)
			replacewith = arguments[arg]
			query = query.replace(find, replacewith)
		return query


dboperator_instance = dboperator()

try:
	print('Подключение к базе данных...')
	dboperator_instance.connect()
except Exception as e:
	print(e)
	print('Connection to MySQL database failed. Application stopped')
	exit(0)
else:
	print('Подключение установлено.')
