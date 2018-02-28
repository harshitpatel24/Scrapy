# -*- coding: utf-8 -*-
import scrapy
import urllib.parse
import json
from azcrawl.items import AzcrawlItem
import time
import sys
import psycopg2
from datetime import datetime
class FetchReviewsSpider(scrapy.Spider):
	name = 'fetch_reviews'
	allowed_domains = ['amazon.in']
	start_urls = ['http://www.amazon.in/']
	total_review_count={}
	
	def parse(self, response):
		#if we have particular product_id then below two lines give all review for products
		self.logger.info('Start Crawling for single product given into program')
		url='https://www.amazon.in/product-reviews/' +'B0719KYGMQ'+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
		yield scrapy.Request(url,callback=self.parse_product_page)
		
		'''
		#collect all reviews for all product_ids which are available in product_details table
		self.logger.info('Start Crawling for all available products in DB')
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
		cur=conn.cursor()
		cur.execute("SELECT product_id from product_details")
		product_ids=cur.fetchall()
		for product_id in product_ids:
			url='https://www.amazon.in/product-reviews/'+str(product_id[0])+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
			yield scrapy.Request(url,callback=self.parse_product_page)
		cur.close()
		'''
		'''
		#another option if not DB then json file read & get ids
		self.logger.info('Start Crawling for all available products in json file')
		try:
			with open(str(self.filename)) as json_file:  
				data = json.load(json_file)
				for p in data:
					#print("start crawling for :" + p['product_id']+"  :  "+ p['product_name'])        
					product_id = str(p['product_id'])
					url='https://www.amazon.in/product-reviews/' +product_id+'/ref=cm_cr_arp_d_viewopt_rvwer?ie=UTF8&showViewpoints=1&reviewerType=all_reviews&sortBy=recent&pageNumber=1'
					yield scrapy.Request(url,callback=self.parse_product_page)
					#time.sleep(120) #start crawling for another product after 2 minutes
		except:
			print("No product to crawl")
		'''	
	
	def parse_product_page(self, response):
		#taking product_id from response_url
		res_url=str(response.url)
		find_pos=res_url.find("product-reviews/")
		product_id= str(res_url[find_pos+16 : find_pos+26])
		self.total_review_count[str(product_id)]=int(0)
		#start extracting reviews
		self.logger.info('Start Extracting Reviews')
		yield from self.extract_pages(response)
		yield from self.extract_reviews(response)
	
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
			
			conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
			cur1=conn.cursor()
			cur1.execute("SELECT source_id from source where source_name like 'amazon'")
			source_id=cur1.fetchone()
			self.total_review_count[str(product_id)]+=1
			
			items['source_id'] = int(source_id[0])
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
		page_links = response.css('span[data-action="reviews:page-action"] li')
		base_parts = urllib.parse.urlsplit(response.url)
		if len(page_links) > 2:
			last_page_url = page_links[-2].css('a::attr(href)').extract_first()
			url_parts = urllib.parse.urlsplit(last_page_url)
			qs = urllib.parse.parse_qs(url_parts.query)
			last_page_number = int(qs.get('pageNumber', [1])[0])
			self.logger.info('last page number ' + repr(last_page_number))
			if last_page_number > 1:
				url_parts = list(url_parts)
				url_parts[0] = base_parts.scheme
				url_parts[1] = base_parts.netloc
				url_parts[3] = qs
				for i in range(2, last_page_number + 1):
					qs["pageNumber"] = i
					url_parts[3] = urllib.parse.urlencode(qs, doseq=True)
					self.logger.info('Last Review page number: ' + str(last_page_number))
					self.logger.info('Url: ' + repr(url_parts))
					yield scrapy.Request(urllib.parse.urlunsplit(url_parts), self.parse_reviews)
					time.sleep(120)

	def closed(self,reason):
		self.logger.info('Closing Spider and starting insert product with review_count in crawler table')
		for k,v in self.total_review_count.items():
			conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
			cur1=conn.cursor()
			cur1.execute("SELECT source_id from source where source_name like 'amazon'")
			source_id=cur1.fetchone()
			now=datetime.now()
			date1=now.strftime("%d_%m_%Y")
			time1=now.strftime("%H_%M_%S")
			cur3=conn.cursor()
			cur3.execute("INSERT INTO crawler(source_id,product_id,review_count,date,time) VALUES ({0},'{1}',{2},'{3}','{4}')".format(int(source_id[0]),str(k),int(v),str(date1),str(time1)))
			conn.commit()
			cur3.close()
			cur1.close()
			self.logger.info(str(k)+' entry done successfully')
