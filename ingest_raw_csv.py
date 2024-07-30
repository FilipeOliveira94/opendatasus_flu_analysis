import requests
from bs4 import BeautifulSoup
import os

def ingest():
    
    def get_request(url):
        s = requests.Session()
        retries = requests.adapters.Retry(total=5, backoff_factor=1)
        s.mount('http://', requests.adapters.HTTPAdapter(max_retries=retries))
        print(f'Requesting URL: {url}')
        response = s.get(url)
        return response

    domain = 'https://opendatasus.saude.gov.br'
    base_url = '/organization/ministerio-da-saude'
    response = get_request(domain+base_url)

    # Getting page count
    soup = BeautifulSoup(response.text, 'html.parser')
    dataset_pages = soup.find_all('a', attrs={'class':'page-link'})
    pagecount = int(dataset_pages[-2].get_text())

    catalog_pages = []
    for page in range(1, pagecount):
        page_url = domain+ base_url + f'?page={page}'
        response = get_request(page_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        [catalog_pages.append(domain + link['href']) for link in soup.find_all('a')
        if ('Notificações de Síndrome Gripal' in link.get_text()
            and 'API' not in link.get_text())]
        
    dataset_pages = []
    for page in catalog_pages:
        response = get_request(page)
        soup = BeautifulSoup(response.text, 'html.parser')
        [dataset_pages.append(domain + link['href']) for link in soup.find_all('a', {'class':'heading'})
        if ('Dados' in link['title'] 
            and 'Dicionário' not in link['title'])] 
        
    csv_links = []
    for page in dataset_pages:
        response = get_request(page)
        soup = BeautifulSoup(response.text, 'html.parser')
        [csv_links.append(link['href']) for link in soup.find_all('a', href=True)
        if 'csv' in link['href']]
        
    csv_count = 1
    os.mkdir('data')
    for link in csv_links:
            print(f'{csv_count} - Reading data from {link}')
            csv_count += 1
            response = get_request(link)
            filepath = f'data/{link.split('/')[5] + '-' + link.split('/')[6][-2:] + '-' + link.split('/')[7]}.csv'
            with open(filepath,'wb') as f:
                f.write(response.content)
            
def main():
    ingest()
    
if __name__ == "__main__":
    main()