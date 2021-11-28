from sqlalchemy import create_engine
from bs4        import BeautifulSoup
from datetime   import datetime
from os.path    import exists
from os         import mkdir
import pandas   as pd
import requests
import sqlite3
import math
import re
import logging
from time import sleep
import numpy as np


class HmMensJeans:
    '''
    --> Collect information about men's jeans on H&M page
    '''
    def __init__(self, url_full_page=None, product_base=pd.DataFrame(), product_details=pd.DataFrame(), database='data/hm_db.sqlite', 
                path='/home/marcos/Documents/ds_ao_dev/',
                header={'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}):
        '''
        :param url_full_page: URL with full product section page
        :param product_base: Basic information about product top page
        :param product_details: DataFrame with product information at composition level
        :param database: Sqlite database to store data
        :param path: The local directory of project
        :param header: Header to acess web page
        '''
        self._url_full_page = url_full_page
        self._product_base = product_base
        self._product_details = product_details
        self._database = database
        self._path = path
        self._header=header


    def url_full_page(self, url='https://www2.hm.com/en_us/men/products/jeans.html'):
        '''
        --> Generate the URL full product page

        :param url: URL patter web page
        '''
        # HTML
        page = requests.get(url, headers=self._header)

        # BeautifulSoup html page
        soup = BeautifulSoup(page.text, features='html.parser')

        # Get total products 
        show_items = int(soup.find('h2', class_ = 'load-more-heading').get('data-items-shown'))
        total_items = int(soup.find('h2', class_ = 'load-more-heading').get('data-total'))
        page_number = math.ceil(total_items / show_items)

        # Final URL
        self._url_full_page = url + '?page-size=' + str(page_number * show_items)
        self.loggin('info', 'URL full page done')

    
    def product_base(self):
        '''
        --> Take the code of product top page
        '''
        # HTML
        page = requests.get(self._url_full_page, headers=self._header)

        # BeautifulSoup html page
        soup = BeautifulSoup(page.text, features='html.parser')

        # BeautifulSoup html products content
        products = soup.find('ul', class_="products-listing small")
        product_list = products.find_all('article', class_='hm-product-item')

        # product id
        product_id = [p.get('data-articlecode') for p in product_list]
        self._product_base['product_id'] = product_id

        # Loggin
        self.loggin('info', 'Product base collecting done')


    def product_details(self):
        '''
        --> Collect product information in collor and composition level
        '''
        time_to_sleep = np.arange(30, 1000, 30)
        count = 0
        df_pattern = pd.DataFrame(columns=['Art. No.', 'Composition', 'Fit', 'Size'])
        # Job 1
        for code in self._product_base['product_id']:
            count += 1
            if count in time_to_sleep:
                sleep(10.0)
                # Call loggin
                self.loggin('info', 'Sleep for 10 seconds')
            # URL composition and request
            url = f"https://www2.hm.com/en_us/productpage.{code}.html"

            # Debug information of last product
            self.loggin('debug', f'Product: {url}')

            #-----------------------------Page requests----------------------------
            page = requests.get(url, headers=self._header).text
            
            #-----------------------------BeautifulSoup Object---------------------
            soup = BeautifulSoup(page, features='html.parser')
            
            #-----------------------------Product Color and Code-------------------

            product_info = soup.find_all('a', class_ = 'filter-option miniature') + soup.find_all('a', class_ = 'filter-option miniature active')
            product_color = [p.get('data-color') for p in product_info]
            product_id = [p.get('data-articlecode') for p in product_info]

            df_color = pd.DataFrame({'product_id': product_id, 'product_color': product_color})

            #------------------------------Product Details Job 2---------------------
            # Get composition information
            for code in df_color['product_id']:
                url = (f"https://www2.hm.com/en_us/productpage.{code}.html")

                #----------------------------Debbug message--------------------------
                self.loggin('debug', f'Collor: {url}')
            
                #----------------------------Request and soup object------------------
                page = requests.get(url, headers=self._header).text
                soup = BeautifulSoup(page, features='html.parser')
                    
                #-----------------------------Product name-----------------------------
                product_name = soup.find('h1', class_ = 'primary product-item-headline').get_text()

                #-----------------------------Product price----------------------------
                product_price = soup.find('div', class_ = 'primary-row product-item-price').get_text()
                
                #-----------------------------Product composition----------------------
                product_composition = [list(filter(None, p.get_text().split('\n'))) for p in soup.find_all('div', class_ = 'pdp-description-list-item')]
                
                # Composition to DataFrame
                df_composition = pd.DataFrame(product_composition).T
                df_composition.columns = df_composition.iloc[0]
                df_composition.drop(index=0, inplace=True)
                
                # Add name and price columns
                df_composition['name'] = product_name
                df_composition['price'] = product_price
            
                # Garantee the patters columns
                df_composition = pd.concat([df_pattern, df_composition])
                
                # Rename columns and fill data below
                df_composition.columns = df_composition.columns.str.lower()
                df_composition = df_composition[['art. no.', 'name', 'price', 'composition', 'fit', 'size']]
                df_composition.columns = ['product_id', 'name', 'price', 'composition', 'fit', 'size']
                df_composition.fillna(method='ffill', inplace=True)    
            
                # Ignoring Pocket lining and lining composition
                df_composition = df_composition[~df_composition['composition'].str.contains('Pocket lining')]
                df_composition = df_composition[~df_composition['composition'].str.contains('Lining')]
                
                #------------------------------Product details-----------------------
                self._product_details = pd.merge(df_composition, df_color, how='left', on='product_id')
                self._product_details['scrapy_datetime'] = datetime.now().strftime('%Y-%m-%d')

                #------------------------------Clean data Job 3----------------------
                self.data_cleaning()
                
                #------------------------------Data Store Job 5----------------------
                self.database()
                #------------------------------All products--------------------------
                #self._product_details = pd.concat([self._product_details, df_sku], axis=0).reset_index(drop=True)
                #self._product_details.drop_duplicates(inplace=True)
                

        # Event logging
        self.loggin('info', 'Data collection product details done')


    def data_cleaning(self):
        '''
        --> Clean the data before store in database
        '''
        # name
        self._product_details['name'] = self._product_details['name'].apply(lambda x: x.replace('\n', '').replace('\t', '').strip().replace(' ', '_').lower())

        # price
        self._product_details['price'] = self._product_details['price'].apply(lambda x: float(x.replace('\n', '').replace('\r', '').strip().replace('$', '')))

        # scrapy_datetime
        self._product_details['scrapy_datetime'] = pd.to_datetime(self._product_details['scrapy_datetime'], format='%Y-%m-%d')

        # fit
        self._product_details['fit'] = self._product_details['fit'].apply(lambda x: x.lower().replace(' ', '_'))

        # product color
        self._product_details['product_color'] = self._product_details['product_color'].apply(lambda x: x.replace(' ', '_').lower())

        # size number
        self._product_details['size_number'] = self._product_details['size'].apply(lambda x: re.search('(\d{3})cm', x).group(1) if pd.notnull(x) else x)

        # size model
        try:
            self._product_details['size_model'] = self._product_details['size'].apply(lambda x: re.search('\d{2}/\d{2}', x).group(0) if pd.notnull(x) else x) 
        except:
            self._product_details['size_model'] = self._product_details['size'].apply(lambda x: re.search('\d{2}$|\w$', x).group(0) if pd.notnull(x) else x)

        # Composition 
        df_composition = self._product_details['composition'].str.split(',', expand=True).reset_index(drop=True)
        df_composition = df_composition.apply(lambda x: x.str.strip())

        #--------------------------cotton----------------------------
        self._product_details['cotton'] = df_composition[0]

        #--------------------------spandex---------------------------
        try:
            self._product_details['spandex'] = df_composition[1]
        except:
            self._product_details['spandex'] = None

        #Composition with only numbers
        self._product_details['cotton'] = self._product_details['cotton'].apply(lambda x: float(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)
        self._product_details['spandex'] = self._product_details['spandex'].apply(lambda x: float(re.search('\d+', x).group(0)) / 100 if pd.notnull(x) else x)

        #Drop orinial composition and size columns
        self._product_details.drop(columns=['composition', 'size'], inplace=True)

        # Event loggin
        self.loggin('info', 'Data cleaning done')

    
    def query_db(self, query):
        '''
        --> Do a database query
        
        :param query: SQL command to be executed
        :param database: Database to manipulate
        '''
        con = sqlite3.connect(self._path + self._database)
        cur = con.cursor()
        cur.execute(query)
        con.commit()
        con.close()

    
    def database(self, table_name='mens_jeans'):
        '''
        --> Store informaiton on sqlite

        :param database: Database to manipulate
        :param table_name: The name of a table
        '''
        # Reorder columns
        df_store = self._product_details[['product_id', 'name', 'price', 'product_color', 'fit', 'size_number', 'size_model',
                            'cotton', 'spandex', 'scrapy_datetime']]

        # Connect to database
        db = create_engine('sqlite:///' + self._path + self._database, echo=False)
        conn = db.connect()

        # Create a database and table
        if not exists(self._path + self._database):
              # Schema
            query_showroom = f'''    
                CREATE TABLE {table_name} (
                    product_id       TEXT PRIMARY KEY,
                    name             TEXT,
                    price            REAL,
                    product_color    TEXT,
                    fit              TEXT,
                    size_number      INTERGER,
                    size_model       TEXT,
                    cotton           REAL,
                    spandex          REAL,
                    scrapy_datetime  TEXT
                )
            '''
            self.query_db(query_showroom)

        # Job 4
        product_id = df_store['product_id'][0]
        price = df_store['price'][0]
        check_code = f'SELECT * FROM {table_name} where product_id = "{product_id}";'
        check_price = f'SELECT * FROM {table_name} where product_id = "{product_id}" and price = {price};'
        
        if pd.read_sql(check_code, conn).empty:
            # Insert new product
            try:
                df_store.to_sql(name=table_name, con=conn, if_exists='append', index=False)
                # Call loggin
                self.loggin('info', 'Data storing done')
            except:
                # Call loggin
                self.loggin('info', f'{product_id} not inserted!')
        elif pd.read_sql(check_price, conn).empty:
            # Update product information
            query_update = f'UPDATE {table_name} SET price = {price} WHERE product_id = "{product_id}";'
            self.query_db(query_update)
            # Call Loggin
            self.loggin('info', 'Data updated done')
        else:
            # Call loggin
            self.loggin('info', 'The product already exists and the price has not changed')


    def loggin(self, severity_level=str, message=str):
        '''
        --> Register the event about system operations

        :param severity_level: Define the level of severity log
            info      - Status execution
            debug     - Useful information to debbug system
            warning   - Alert about system performance
            error     - Ocurence of execution not working
            critical  - Compromised system integrity
        :param message: The text to be loggin
        '''
        log_path = self._path + 'logs'
        if not exists(log_path):
            mkdir(log_path)

        logging.basicConfig(
            filename = log_path + '/webscraping_hm.log',
            level    = logging.DEBUG,
            format   = '%(asctime)s - %(levelname)s - %(name)s - %(message)s',
            datefmt  = '%Y-%m-%d %H:%M:%S'  
        )

        logger = logging.getLogger('webscraping_hm')

        if severity_level == 'info':
            logger.info(message)
        elif severity_level == 'debug':
            logger.debug(message)
        elif severity_level == 'warning':
            logger.debug(message)
        elif severity_level == 'error':
            logger.error(message)
        elif severity_level == 'critical':
            logger.critical(message)