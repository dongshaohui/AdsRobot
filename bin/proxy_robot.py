# !/usr/bin/python
# -*- coding=utf8 -*-
# author: Shaohui Dong
# desc: 代理服务器刷pv、uv

import urllib2,re,time,urllib,random,user_agents,DB
	
# 连接数据库 
def Connent_Online_Mysql_By_DB(hostname,port,username,pwd,dbname,socket):
    db = DB.DB(False,host=hostname, port=port, user=username ,passwd=pwd, db=dbname,charset='gbk', unix_socket=socket) 
    return db

# 获取代理服务器IP、PORT
def get_proxy_list(db):
	proxy_list = db.select('select ip_address,ip_port,ip_type from proxy_ip_list where ip_type = "http"')
	proxy_addr = []
	for proxy in proxy_list:
		record = {}
		record['http'] = 'http://' + proxy[0] + ':' + proxy[1]
		proxy_addr.append(record)
	return proxy_addr

# 刷PV
def getHtml(db,url):
	print 'getHtml'
	proxy_list = get_proxy_list(db)
	proxy_ip =random.choice(proxy_list) #在proxy_list中随机取一个ip
	print proxy_ip
	proxy_support = urllib2.ProxyHandler(proxy_ip)
	opener = urllib2.build_opener(proxy_support,urllib2.HTTPHandler)
	urllib2.install_opener(opener)
	request = urllib2.Request(url)
	user_agent = random.choice(user_agents.user_agents)  #在user_agents中随机取一个做user_agent
	request.add_header('User-Agent',user_agent) #修改user-Agent字段
	print user_agent
	html = urllib2.urlopen(request).read()
	return proxy_ip
	
def main(db):
	urls = ['http://adjokes.10jokes.com:3001/joke_ad/ad08']
	count_True,count_False,count= 0,0,0
	while True:
		for url in urls:
			count +=1
			proxy_ip = None
			try:
				proxy_ip=getHtml(db,url)            
			except urllib2.URLError:
				print 'URLError! The bad proxy is %s' %proxy_ip
				count_False += 1
			except urllib2.HTTPError:
				print 'HTTPError! The bad proxy is %s' %proxy_ip
				count_False += 1
			except:
				print 'Unknown Errors! The bad proxy is %s ' %proxy_ip 
				count_False += 1
			randomTime = random.uniform(1,3) #取1-10之间的随机浮点数
			time.sleep(randomTime) #随机等待时间
			print '%d Eroors,%d ok,总数 %d' %(count_False,count - count_False,count)

if __name__ == '__main__':
	# db connector
	db = Connent_Online_Mysql_By_DB('rdsjjuvbqjjuvbqout.mysql.rds.aliyuncs.com',3306,'dongsh','5561225','ad_robot_db','/tmp/mysql.sock')
	main(db)
