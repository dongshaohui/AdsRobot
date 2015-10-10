#!/usr/local/python/bin
# coding=utf-8

# Can be 'Prototype', 'Development', 'Product'
__status__ = 'Development'

import sys
import MySQLdb

#from pypet.common import log

class DB():
	'''A simple	database query interface.'''
	def __init__(self, auto_commit,	**kwargs):
		if 'charset' not in	kwargs:
			kwargs['charset'] =	'utf8'

		self.conn =	MySQLdb.connect(**kwargs)
		self.cursor	= self.conn.cursor()
		self.autocommit(auto_commit)

	def execute(self, sql, args	= None):
		return self.cursor.execute(sql,	args)

	def executemany(self, sql, args):
		'''Execute a multi-row query.'''
		return self.cursor.executemany(sql,	args)

	def select(self, sql, args = None):
		self.execute(sql, args)
		return self.get_rows()

	def insert(self, table,	column_dict):
		keys = '`,`'.join(column_dict.keys())
		values = column_dict.values()
		placeholder	= ','.join([ '%s' for v	in column_dict.values()	])
		ins_sql	= 'INSERT INTO %(table)s (`%(keys)s`) VALUES (%(placeholder)s)'

		return self.execute(ins_sql	% locals(),	values)

	def multi_insert(self, table, column_name, column_value):
		'''Execute a multi-row insert, the same	as executemany'''
		keys = '`,`'.join(column_name)
		placeholder = ','.join([ '%s' for v	in column_name ])
		mins_sql = 'INSERT INTO %(table)s (`%(keys)s`) VALUES (%(placeholder)s)'

		return self.cursor.executemany(mins_sql % locals(), column_value)

	def replace(self, table, column_dict):
		keys = '`,`'.join(column_dict.keys())
		values = column_dict.values()
		placeholder	= ','.join([ '%s' for v	in column_dict.values()	])
		repl_sql = 'REPLACE	INTO %(table)s (`%(keys)s`)	VALUES (%(placeholder)s)'

		return self.execute(repl_sql % locals(), values)

	def update(self, table,	column_dict, cond_dict):
		set_stmt = ','.join([ '%s=%%s' % k for k in	column_dict.keys() ])
		cond_stmt =	','.join([ '%s=%%s'	% k	for	k in cond_dict.keys() ])
		args = column_dict.values()	+ cond_dict.values()
		upd_sql	= 'UPDATE %(table)s	set	%(set_stmt)s where %(cond_stmt)s'

		return self.execute(upd_sql	% locals(),	args)

	def delete(self, table,	cond_dict):
		cond_stmt =	','.join([ '%s=%%s'	% k	for	k in cond_dict.keys() ])
		del_sql	= 'DELETE FROM %(table)s where %(cond_stmt)s'

		return self.execute(del_sql	% locals(),	cond_dict.values())

	def delete_table(self, table):
		del_sql	= 'DELETE FROM %(table)s '

		return self.execute(del_sql	% locals())

	def get_rows(self, size	= None,	is_dict	= False):
		if size	is None:
			rows = self.cursor.fetchall()
		else:
			rows = self.cursor.fetchmany(size)

		if rows	is None:
			rows = []

		if is_dict:
			dict_rows =	[]
			dict_keys =	[ r[0] for r in	self.cursor.description	]

			for	row	in rows:
				print row, dict_keys
				print zip(dict_keys, row)
				dict_rows.append(dict(zip(dict_keys, row)))

			rows = dict_rows

		return rows

	def get_rows_num(self):
		return self.cursor.rowcount

	def get_mysql_version(self):
		MySQLdb.get_client_info()

	def autocommit(self, flag):
		self.conn.autocommit(flag)

	def commit(self):
		'''Commits the current transaction.'''
		self.conn.commit()

	def __del__(self):
		#self.commit()
		self.close()

	def close(self):
		self.cursor.close()
		self.conn.close()
