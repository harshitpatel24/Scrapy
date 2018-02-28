# -*- coding: utf-8 -*-
import scrapy
import urllib.parse
import json
from azcrawl.items import AzcrawlItem
import time
import sys
import psycopg2
import math
from datetime import datetime
class AzCursorSpider(scrapy.Spider):
	name = 'az_cursor'
	allowed_domains = ['amazon.in']
	start_urls = ['http://amazon.in/']
	new_review_count_dir={}
	current_review_count_dir={}	
	
	def parse(self, response):
		#if we have particular product_id then below two lines give all review for products
		self.logger.info('Start Crawling Only for New (Delta) Reviews for given product_id in the program')
		url='https://www.amazon.in/product-reviews/' +'B0719KYGMQ'+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
		yield scrapy.Request(url,callback=self.parse_product_page)		
		
		'''collect all reviews for all product_ids which are available in product_details table
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
		self.logger.info('Database Connection Established')
		cur=conn.cursor()
		cur.execute("SELECT product_id from product_details")
		product_ids=cur.fetchall()
		self.logger.info('Start Crawling Only for New (Delta) Reviews for availabe product_id in DB')
		for product_id in product_ids:
			url='https://www.amazon.in/product-reviews/'+str(product_id[0])+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
			yield scrapy.Request(url,callback=self.parse_product_page)
		cur.close()
		'''
		
		'''another option if not DB then json file read & get ids
		try:
			self.logger.info('Start Crawling Only for New (Delta) Reviews for availabe product_id in json file')
			with open(str(self.filename)) as json_file:  
				data = json.load(json_file)
				for p in data:
					#print("start crawling for :" + p['product_id']+"  :  "+ p['product_name'])        
					product_id = str(p['product_id'])
					url='https://www.amazon.in/product-reviews/' +product_id+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
					yield scrapy.Request(url,callback=self.parse_product_page)
					#time.sleep(120) #start crawling for another product after 2 minutes
		except:
			self.logger.info('No data in json file or File is not available')
			print("No product to crawl")
		'''	
	
	def parse_product_page(self, response):
		#here we check current review_count and stored review_count
		self.logger.info('Checking If new reviews are available or not')
		url_for_product_id=response.url
		product_id=url_for_product_id[url_for_product_id.find('product-reviews/')+16:48]
		current_review_count=response.xpath('//span[@data-hook="total-review-count"]/text()').extract()
		current_review_count=current_review_count[0].replace(',','')
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
		cur1=conn.cursor()
		cur1.execute("SELECT review_count from crawler where (product_id)=('"+str(product_id)+"')")
		stored_review_count=cur1.fetchone()
		if (int(current_review_count) > int(stored_review_count[0])):
			#if new review
			self.logger.info('New Reviews are available for '+ str(product_id))
			yield from self.extract_pages(response)
			yield from self.extract_reviews(response)
		else:
			#if no new review DONE
			now=datetime.now()
			date1=now.strftime("%d_%m_%Y")
			time1=now.strftime("%H_%M_%S")
			cur2 = conn.cursor()
			self.logger.info('Up to date reviews available for '+ str(product_id))
			cur2.execute("UPDATE crawler SET date=%s, time=%s where product_id=%s",(date1,time1,product_id))
			conn.commit()
			self.logger.info('Update date and time for record')
			cur2.close()
		cur1.close()
	
	def parse_reviews(self, response):
		yield from self.extract_reviews(response)
	
	def extract_reviews(self, response):
		for review in response.css('div[data-hook=review]'):
			items = AzcrawlItem()
			res_url=str(response.url)
			find_pos=res_url.find("product-reviews/")
			product_names=response.xpath('//h1[@class="a-size-large a-text-ellipsis"]/a[@data-hook="product-link"]/text()').extract()
			product_id= str(res_url[find_pos+16 : find_pos+26])
			product_name=product_names[0].split("(")[0].strip(" ")
			review_id = review.xpath('@id').extract_first()
			review_title = review.css('a.review-title::text').extract_first()
			review_text = '\n'.join(review.css('span.review-text::text').extract())
			review_date = str(review.css('span.review-date::text').extract_first())[3:]
			stars1 = self.extract_stars(review),
			author_name = review.css('a[data-hook="review-author"]::text').extract_first()
			badges = review.css('span.c7y-badge-text::text').extract()
			comments_count = review.css('span.review-comment-total::text').extract_first()
			
			self.new_review_count_dir[str(product_id)]+=1
			
			conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
			cur1=conn.cursor()
			cur1.execute("SELECT source_id from source where source_name like 'amazon'")
			source_id=cur1.fetchone()
			
			items['source_id'] = source_id[0]
			items['product_id'] = product_id
			items['product_name'] = product_name
			items['review_id'] = review_id
			items['review_title'] = review_title
			items['review_text'] = review_text
			items['review_date'] = review_date
			items['stars'] = stars1[0]
			items['author_name'] = author_name
			items['comments_count'] = comments_count
			yield items
			cur1.close()
			#time.sleep(15)
	
	def extract_review_votes(self, review):
		votes = review.css('span.review-votes::text').extract_first()
		if not votes:
			return 0
		votes = votes.strip().split(' ')
		if not votes:
			return 0
		return votes[0].replace(',', '')
	
	def extract_stars(self, review):
		stars = None
		star_classes = review.css('i.a-icon-star::attr(class)').extract_first().split(' ')
		for i in star_classes:
			if i.startswith('a-star-'):
				stars = int(i[7:])
				break
		return str(stars)
	
	def extract_pages(self, response):
		url_for_product_id=response.url
		product_id=url_for_product_id[url_for_product_id.find('product-reviews/')+16:48]
		current_review_count=response.xpath('//span[@data-hook="total-review-count"]/text()').extract()
		current_review_count=current_review_count[0].replace(',','')
		
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
		cur1=conn.cursor()
		cur1.execute("SELECT review_count from crawler where (product_id)=('"+str(product_id)+"')")
		stored_review_count=cur1.fetchone()
		self.new_review_count_dir[str(product_id)]=int(0)
		self.current_review_count_dir[str(product_id)]=int(current_review_count)
		
		new_review_count=int(current_review_count) - int(stored_review_count[0])
		new_pages=int(math.ceil(float(new_review_count)/10.0))
		self.logger.info('Start Crawling for New Reviews availabe for'+ str(product_id))
		for j in range(2,new_pages+1):
			url='https://www.amazon.in/product-reviews/' +str(product_id)+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber='+str(j)
			yield scrapy.Request(url,callback=self.parse_reviews)
			time.sleep(120)
			
	def closed(self,reason):
		self.logger.info('Spider Closed')
		self.logger.info('Starting Update review_count in DB')
		for k,v in self.new_review_count_dir.items():
			conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
			cur3=conn.cursor()
			cur3.execute("SELECT review_count from crawler where (product_id)=('"+str(k)+"')")
			
			review_count=int(cur3.fetchone()[0])
			actual_new_reviews=int(self.current_review_count_dir[str(k)]) - review_count
			crawled_new_reviews=int(self.new_review_count_dir[str(k)])
			new_count=review_count + actual_new_reviews
			now=datetime.now()
			date1=now.strftime("%d_%m_%Y")
			time1=now.strftime("%H_%M_%S")
			
			if crawled_new_reviews > actual_new_reviews:
				cur2 = conn.cursor()
				cur2.execute("UPDATE crawler SET review_count=%s, date=%s, time=%s where product_id=%s",(str(new_count),date1,time1,str(k)))
				conn.commit()
				cur2.close()
			cur3.close()
			self.logger.info('Successfully Updated review_count for '+ str(k))
		'''
		print(self.new_review_count_dir)
		print(self.current_review_count_dir)
		'''