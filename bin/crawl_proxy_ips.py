# !/usr/bin/python
# -*- coding=utf8 -*-
# author: Shaohui Dong
# desc: 爬取代理IP、端口
import DB
from BeautifulSoup import BeautifulSoup
import sys,os,urllib2,threading

# 连接数据库 
def Connent_Online_Mysql_By_DB(hostname,port,username,pwd,dbname,socket):
    db = DB.DB(False,host=hostname, port=port, user=username ,passwd=pwd, db=dbname,charset='gbk', unix_socket=socket) 
    return db

# 写入数据库
def write_record_db(db,list_obj,table_name):
    try:
        db.insert(table_name,list_obj)
        db.commit()
    except Exception,e:
        print e
		
# 获取代理信息
def fetch_proxy_info(db):
	# clean proxy list
	db.delete_table()
	fetch_anony_proxy_info()

# 获取普通代理信息
def fetch_normal_proxy_info():
	root = 'http://www.xicidaili.com/nt/'

# 获取匿名代理信息
def fetch_anony_proxy_info(db):
	root = 'http://www.xicidaili.com/nn/'
	for i in range(1,100):
		page_link = root + (str)(i)
		curl_cmd = 'curl ' + page_link + ' > ../data/' + (str)(i) + '.data'
		os.system(curl_cmd)
		f = open('../data/' + (str)(i) + '.data' , 'r')
		
		# 解析内容，获取proxy ip
		soup = BeautifulSoup(f.read())
		
		trlist = soup.find('table',{'id':'ip_list'}).findAll('tr')[1:]
		for trinfo in trlist:
			ip_address = trinfo.findAll('td')[2].text
			ip_port = trinfo.findAll('td')[3].text
			ip_type = trinfo.findAll('td')[6].text
			record = {}
			record['ip_address'] = ip_address
			record['ip_port'] = ip_port
			record['ip_type'] = ip_type
			write_record_db(db,record,'proxy_ip_list')
			f.close()
		del_cmd = 'rm -rf ' + '../data/' + (str)(i) + '.data' 
		os.system(del_cmd)

				
if __name__ == '__main__':
	# db connector
	
	db = Connent_Online_Mysql_By_DB('rdsjjuvbqjjuvbqout.mysql.rds.aliyuncs.com',3306,'dongsh','5561225','financal_product','/tmp/mysql.sock')
	fetch_proxy_info(db)