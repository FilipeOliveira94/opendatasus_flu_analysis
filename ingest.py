import requests
from bs4 import BeautifulSoup
import os
from datetime import datetime
import logging
import sys

def ingest(logger):
    def get_request(url):
        s = requests.Session()
        retries = requests.adapters.Retry(total=5, backoff_factor=1)
        s.mount('http://', requests.adapters.HTTPAdapter(max_retries=retries))
        logger.info(f'Requesting URL: {url}')
        response = s.get(url)
        return response

    domain = 'https://opendatasus.saude.gov.br'
    base_url = '/organization/ministerio-da-saude'
    response = get_request(domain+base_url)

    # Getting page count
    soup = BeautifulSoup(response.text, 'html.parser')
    dataset_pages = soup.find_all('a', attrs={'class':'page-link'})
    pagecount = int(dataset_pages[-2].get_text())
    logger.info(f'Got page count: {pagecount}')

    catalog_pages = []
    for page in range(1, pagecount):
        page_url = domain+ base_url + f'?page={page}'
        response = get_request(page_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        [catalog_pages.append(domain + link['href']) for link in soup.find_all('a')
        if ('Notificações de Síndrome Gripal' in link.get_text()
            and 'API' not in link.get_text())]
    if len(catalog_pages) > 0: 
        logger.info(f'SUCCESS - Got {len(catalog_pages)} catalog pages')
        
    dataset_pages = []
    for page in catalog_pages:
        response = get_request(page)
        soup = BeautifulSoup(response.text, 'html.parser')
        [dataset_pages.append(domain + link['href']) for link in soup.find_all('a', {'class':'heading'})
        if ('Dados' in link['title'] 
            and 'Dicionário' not in link['title'])] 
    if len(dataset_pages) > 0:
        logger.info(f'SUCCESS - Got {len(dataset_pages)} dataset pages')
        
    csv_links = []
    for page in dataset_pages:
        response = get_request(page)
        soup = BeautifulSoup(response.text, 'html.parser')
        [csv_links.append(link['href']) for link in soup.find_all('a', href=True)
        if 'csv' in link['href']]
    if len(csv_links) > 0:
        logger.info(f'SUCCESS - Got {len(csv_links)} csv links')
        
    csv_count = 1
    error_count = 0
    os.mkdir('raw_data')
    for link in csv_links:
        logger.info(f'{csv_count} - Reading data from {link}')
        csv_count += 1
        try:
            response = get_request(link)
            filepath = f'raw_data/{link.split('/')[5] + '-' + link.split('/')[6][-2:] + '-' + link.split('/')[7]}.csv'
            with open(filepath,'wb') as f:
                f.write(response.content)
            logger.info(f'SUCCESS writing file: {filepath}')
        except:
            logger.error(f'error writing file: {filepath}')
            error_count += 1
    logger.info(f'Success writing {csv_count - error_count} files')
            
def main():
    logger = logging.getLogger(__name__)
    start_time = datetime.now()
    logging.basicConfig(filename=f'ingest_{start_time.strftime("%Y-%m-%d_%H-%M-%S")}.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    
    logger.info(f'Starting script: {start_time.strftime('%Y-%m-%d %H:%M:%S')}')
    ingest(logger)
    end_time = datetime.now()
    logger.info(f'Ending script: {end_time.strftime('%Y-%m-%d %H:%M:%S')}')
    logger.info(f'Elapsed time (secs): {round((end_time - start_time).total_seconds(),3)}')
    
if __name__ == "__main__":
    main()