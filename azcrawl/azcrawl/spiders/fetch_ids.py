# -*- coding: utf-8 -*-
import scrapy
import urllib.parse
import itertools
import psycopg2
import time
class FetchIdsSpider(scrapy.Spider):
	name = 'fetch_ids'
	allowed_domains = ['amazon.in']
	start_urls = ['https://www.amazon.in']
	product_list=[]
	#start_urls = ['https://www.amazon.in/gp/search/other/ref=lp_1389401031_sa_p_89?rh=n%3A976419031%2Cn%3A%21976420031%2Cn%3A1389401031&bbn=1389401031&pickerToList=lbr_brands_browse-bin&ie=UTF8&qid=1515690014']
	def parse(self, response):
		searches=['%23','a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
		categories=['electronics','computers','apparel','appliances','watches','shoes','luggage','jewelry','kitchen','pets','beauty','hpc','grocery','sporting','toys','automotive','industrial','videogames','gift-cards']
		
		'''
		#for all category
		for category in categories:
			self.logger.info('Start Crawling for all categories with all A to Z searches')
			for search in searches:
				url_to_get_brands='https://www.amazon.in/gp/search/other/ref=sr_in_a_-2?rh=i%3A'+ category +'%2Cn%&pickerToList=brandtextbin&indexField='+search
				yield scrapy.Request(url_to_get_brands,callback=self.parse1)
		'''
		'''
		#for one category with all searcehs
		self.logger.info('Start Crawling for single categories with all A to Z searches')
		for search in searches:
			url_to_get_brands='https://www.amazon.in/gp/search/other/ref=sr_in_a_-2?rh=i%3A'+ categories[0] +'%2Cn%&pickerToList=brandtextbin&indexField='+search
			yield scrapy.Request(url_to_get_brands,callback=self.parse1)
		'''
		
		#for one category with one search
		#for electronics starts with a
		self.logger.info('Start Crawling for Electronics for brands start with A')
		url_to_get_brands='https://www.amazon.in/gp/search/other/ref=sr_in_a_-2?rh=i%3A'+ categories[0] +'%2Cn%&pickerToList=brandtextbin&indexField='+searches[1]
		yield scrapy.Request(url_to_get_brands,callback=self.parse1)
	
	def parse1(self, response):
		self.logger.info('Collecting URLs of each brand')
		links=response.xpath('//a[@class="a-link-normal"]/@href').extract()
		#print(len(links))
		new_links=[]
		for l in links:
			new_links.append(str("https://www.amazon.in"+l))
		'''
		self.logger.info('Start Crawling for product_details with available brands URLs')
		for nl in new_links:
			yield scrapy.Request(str(nl),callback=self.parse_depth_two)
		'''
		indices = [i for i, s in enumerate(new_links) if '%3AApple' in s]
		yield scrapy.Request(new_links[indices[0]],callback=self.parse_depth_two) #whole list of companies

	def parse_depth_two(self,response):
		try:
			#if more than one page Pagination technique 1
			data=response.xpath('//span[@class="pagnDisabled"]/text()').extract()
			self.logger.info('Parse Depth two first try block executed with disabled last page number')
			url_part1='https://www.amazon.in/s/ref=sr_pg_2?fst=as%3Aoff&rh=n%3A976419031%2Cp_89'
			res_url=response.url
			url_part2=res_url[res_url.rfind("%3A"):]
			url_part3='&page='
			conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
			self.logger.info('Parse Depth two first try block executed connection of db successful')
			cur=conn.cursor()
			cur.execute("SELECT source_id from source where source_name like 'amazon'")
			source_id=cur.fetchone()
			self.logger.info('Parse Depth two first try block executed start crawling for all available pages of particular brand')
			for i in range(1,int(data[0])+1):
				time.sleep(60)
				if i == 1:
					product_id=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]/@href').extract()
					product_name=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]// h2[@class="a-size-base s-inline  s-access-title  a-text-normal"]/text()').extract()
					for l1,l2 in zip(product_id,product_name):
						split_product=l2.split("(")
						if split_product[0] not in self.product_list:
							self.product_list.append(split_product[0])
							yield {
								'source_id':source_id[0],
								'product_id': str(l1[l1.rfind('/')+1:]),
								'product_name': l2.split("(")[0].strip(" "),
								'product_url': "https://www.amazon.in/dp/"+ str(l1[l1.rfind('/')+1:])								
							}
				else:
					whole_url = url_part1 + url_part2 + url_part3 + str(i)
					yield scrapy.Request(whole_url,callback=self.parse_depth_two_products)		
		except:
			try:
				#if more than one page Pagination technique 2
				page_links = response.css('div[class="pagnHy"] span')
				self.logger.info('Parse Depth two except then try block executed')
				last_page_url = page_links[-4].css('a::attr(href)').extract()
				url_parts = urllib.parse.urlsplit(last_page_url[0])
				qs = urllib.parse.parse_qs(url_parts.query)
				last_page_number = int(qs.get('page', [1])[0])
				url_part1='https://www.amazon.in/s/ref=sr_pg_2?fst=as%3Aoff&rh=n%3A976419031%2Cp_89'
				res_url=response.url
				url_part2=res_url[res_url.rfind("%3A"):]
				url_part3='&page='
				conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
				self.logger.info('Parse Depth two except then try block executed connection of db successful')
				cur=conn.cursor()	
				cur.execute("SELECT source_id from source where source_name like 'amazon'")
				source_id=cur.fetchone()
				self.logger.info('Parse Depth two except then try executed start crawling for all available pages of particular brand')
				for i in range(1,int(last_page_number)+1):
					time.sleep(60)
					if i == 1:
						product_id=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]/@href').extract()
						product_name=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]// h2[@class="a-size-base s-inline  s-access-title  a-text-normal"]/text()').extract()
						for l1,l2 in zip(product_id,product_name):
							split_product=l2.split("(")
							if split_product[0] not in self.product_list:
								self.product_list.append(split_product[0])
								yield {
									'source_id':source_id[0],
									'product_id': str(l1[l1.rfind('/')+1:]),
									'product_name': l2.split("(")[0].strip(" "),
									'product_url': "https://www.amazon.in/dp/"+ str(l1[l1.rfind('/')+1:])								
								}
					else:
						whole_url = url_part1 + url_part2 + url_part3 + str(i)
						yield scrapy.Request(whole_url,callback=self.parse_depth_two_products)		
			except:
				#only one page
				self.logger.info('Parse Depth two except then except block executed')
				product_id=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]/@href').extract()
				product_name=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]// h2[@class="a-size-base s-inline  s-access-title  a-text-normal"]/text()').extract()
				conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
				cur=conn.cursor()
				cur.execute("SELECT source_id from source where source_name like 'amazon'")
				source_id=cur.fetchone()
				for l1,l2 in zip(product_id,product_name):
					split_product=l2.split("(")
					if split_product[0] not in self.product_list:
						self.product_list.append(split_product[0])
						yield {
							'source_id':source_id[0],
							'product_id': str(l1[l1.rfind('/')+1:]),
							'product_name': l2.split("(")[0].strip(" "),
							'product_url': "https://www.amazon.in/dp/"+ str(l1[l1.rfind('/')+1:])								
						}
	
	def parse_depth_two_products(self,response):
		product_id=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]/@href').extract()
		product_name=response.xpath('//a[@class="a-link-normal s-access-detail-page  s-color-twister-title-link a-text-normal"]// h2[@class="a-size-base s-inline  s-access-title  a-text-normal"]/text()').extract()
		conn=psycopg2.connect(dbname='crawl_db',user='sanlp_2018',password='2018_sanlp')
		cur=conn.cursor()
		cur.execute("SELECT source_id from source where source_name like 'amazon'")
		source_id=cur.fetchone()
		for l1,l2 in zip(product_id,product_name):
			split_product=l2.split("(")
			if split_product[0] not in self.product_list:
				self.product_list.append(split_product[0])
				yield {
					'source_id':source_id[0],
					'product_id': str(l1[l1.rfind('/')+1:]),
					'product_name': l2.split("(")[0].strip(" "),
					'product_url': "https://www.amazon.in/dp/"+ str(l1[l1.rfind('/')+1:])								
				}
