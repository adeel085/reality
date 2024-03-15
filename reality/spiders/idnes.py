import time
import scrapy
from urllib.parse import urlparse, parse_qs
import boto3
from botocore.exceptions import ClientError
import os
class IdnesSpider(scrapy.Spider):
    name = "idnes"
    allowed_domains = ["reality.idnes.cz"]
    start_urls = ["https://reality.idnes.cz/"]
    custom_settings = {
        'FEED_FORMAT': 'csv',
        'FEED_URI': 'reality.csv',
    }
    if os.path.exists('reality.csv'):
        os.remove('reality.csv')
    def parse(self, response):
        tags=response.css('div.b-tags a.items__item::attr(href)').getall()
        for tag in tags:
            yield scrapy.Request(f"https://reality.idnes.cz{tag}", callback=self.After_select_category)

    def After_select_category(self, response):
        product_links=response.css('div.c-products__inner a.c-products__link::attr(href)').getall()
        last_a_tag = response.css('p.paginator a.btn.paging__item.next').css('a.btn.paging__item.next::attr(href)').get()
        parsed_url = urlparse(last_a_tag)
        query_params = parse_qs(parsed_url.query)
        page_value = query_params.get('page', None)
        for product_link in product_links:
            yield scrapy.Request(product_link, callback=self.Get_data)
        if page_value:
            url=response.urljoin(f"?page={page_value[0]}")
            print(url)
            yield scrapy.Request(url, callback=self.After_select_category)
    def Get_data(self,response):
        title_text = response.css('h1.b-detail__title span::text').get()
        info_text = response.css('p.b-detail__info::text').get().replace("\n","")
        price_text = response.css('p.b-detail__price strong::text').get()
        images = response.css('img[width="1018"][height="600"]::attr(src)').get()
        data={}
        data['title'] = title_text
        data['info'] = ''.join(info_text.split()) if info_text else None
        data['price_text'] = ''.join(price_text.split()) if info_text else None
        data['details_page_url']=response.url
        data['imag']=images
        time.sleep(2)
        yield  data
    def closed(self, reason):
        self.logger.info('Spider closed:Hello %s' % reason)
        self.upload_file()
    def upload_file(self):
        file_name='reality.csv'
        bucket="reality-record"
        object_name="reality.csv"
        print("Testing")
        if object_name is None:
            object_name = os.path.basename(file_name)
        s3_client = boto3.client('s3',
                                 aws_access_key_id="AKIAZQ3DU2CCYENIH6ML",
                                 aws_secret_access_key="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")
        try:
            s3_client.head_object(Bucket=bucket, Key=object_name)
            s3_client.delete_object(Bucket=bucket, Key=object_name)
            print(f"Deleted existing object: {object_name}")
        except ClientError as e:
            # Ignore error if the object does not exist
            print("Error deleting object")
            if e.response['Error']['Code'] != '404':
                print(f"This is Error :{e}",)
                return False
        try:
            with open(file_name, "rb") as f:
                s3_client.upload_fileobj(f, bucket, object_name)
            print(f"Uploaded new file: {object_name}")
        except ClientError as e:
            print(f"Error: {e}")
            return False

        return True
