# Scrapy
Making Script to crawl reviews from E-commerce websites

To run crawler

first install python 3.0+
then install PostgreSQL
pip install Scrapy 
pip install psycopg
then clone whole project
then move to azcrawl folder

open CMD or Terminal

1) go to DB_Layer directory
open DB_Schema.txt
create table inside PostgreSQL as methioned there.

2) now open python in command line mode
>>import Database_Layer as dl
>>dl.add_new_source('amazon')
It will give one id to amazon crawler.

3) Fetch Product ids for any caegory like electronics, fashion, health care, many more....
#scrapy crawl fetch_ids -o (filename).json

All ids are stored in given json file with product name and urls.
Logs related spider are stored inside Logs folder with date and time.

4) now open python in command line mode
>>import Database_Layer as dl
>>dl.add_product_detail('filename.json')
give exact same json file name as we crawl in step-3.
Now all product related data is available inside PostgreSQL database.

5) now move back to main azcrawl directory
#scrapy crawl fetch_reviews -o (reviews.json)

All reviews for available product_ids available inside PostgreSQL database is now available in reviews.json

6) Now on, if we want to crawl reviews after some time then,
#scrapy crawl az_cursor -o (new_reviews.json)

It will give only new reviews which are available on Amazon after our last crawl.
