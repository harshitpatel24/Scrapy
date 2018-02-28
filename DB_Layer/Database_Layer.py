import psycopg2
import json
def add_new_source(source_name):
	try:
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
	except:
		print("Database Connection Failed.....")
	#sql="INSERT INTO source(source_name) VALUES (%s)",source_name
	cur=conn.cursor()
	try:
		cur.execute("INSERT INTO source(source_name) VALUES ('{0}')".format(str(source_name)))
	except:
		print('You should write like add_new_source("amazon") for inserting new source')
	conn.commit()
	cur.close()
	
def add_product_detail(filename):
	try:
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
	except:
		print("Database Connection Failed.....")
	with open(str(filename)) as json_file:  
		data = json.load(json_file)
		for p in data:
			#print("start crawling for :" + p['product_id']+"  :  "+ p['product_name'])        
			source_id = str(p['source_id'])
			product_id = str(p['product_id'])
			product_name=str(p['product_name'])
			product_url=str(p['product_url'])
			cur=conn.cursor()
			try:
				cur.execute("INSERT INTO product_details(source_id,product_id,product_name,product_url) VALUES ({0},'{1}','{2}','{3}')".format(int(source_id),str(product_id),str(product_name),str(product_url)))
			except:
				print("Unable to add "+product_id+"\n Reason: Already Exist or Not proper format")
			conn.commit()
			cur.close()		