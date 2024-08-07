# -*- coding: utf-8 -*-
"""
Módulo: manual_crawler_elfinanciero.py

Descripción:
    Este módulo permite hacer 

Autor:
    Alberto Isaac Pico Lara
    alberto.pico@hsbc.com.mx

Fecha de creación:
    5/08/2024

Versión:
    1.0

Dependencias:
    ver requirements.txt

Licencia:
    MIT License

Historial de cambios:
    Ver github
"""

import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import os

azul = "\33[1;36m"
gris = "\33[0;37m"
blanco = "\33[1;37m"

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)

class Crawler:
    """A web crawler for efficiently scraping data and following links.

    This class provides methods to download web pages, extract linked URLs,
    and scrape specific data from El Financiero articles.

    Attributes:
        visited_urls (list): A list of URLs that have already been visited.
        urls_to_visit (list): A queue of URLs that have yet to be visited.
    """

    def __init__(self, urls=[]):
        """Initializes the crawler with a starting list of URLs.

        Args:
            urls (list, optional): A list of URLs to begin crawling from.
                Defaults to [].
        """
        self.visited_urls = []
        self.urls_to_visit = urls

    def download_url(self, url):
        """Downloads the content of a given URL.

        Args:
            url (str): The URL to download.

        Returns:
            str: The downloaded HTML content.
        """
        return requests.get(url).text

    def get_linked_urls(self, url, html):
        """Extracts URLs linked from a given web page.

        Args:
            url (str): The base URL of the web page.
            html (str): The HTML content of the web page.

        Yields:
            str: Each valid linked URL found on the page.
        """
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def add_url_to_visit(self, url):
        """Adds a URL to the queue of URLs to be crawled.

        Args:
            url (str): The URL to add to the queue.
        """

        if url not in self.visited_urls and url not in self.urls_to_visit:
            self.urls_to_visit.append(url)

    def crawl(self, url):
        """Crawls a given URL and adds linked URLs to the queue.

        Args:
            url (str): The URL to crawl.
        """
        html = self.download_url(url)
        for url in self.get_linked_urls(url, html):
            self.add_url_to_visit(url)

    def datos_ElFinanciero(self, url):
        """Scrapes data from El Financiero articles.

        This method specifically targets El Financiero articles and extracts
        the following information:

            - URL (from response object)
            - Date
            - Title
            - Content

        Args:
            url (str): The URL of the El Financiero article to scrape.

        Returns:
            dict: A dictionary containing the scraped data, or an error message
                if scraping fails.
        """
        # inicialización de variable de salida
        d = {}
        # cabecera de la petición HTTPS
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:128.0) Gecko/20100101 Firefox/128.0"
        }
        # se realiza la petición
        print(f'{azul}Realizando la petición: {blanco}{url}{gris}')
        req = requests.get(url=url, headers=headers, timeout=5)
        print(f'{azul}Código de respuesta...: {blanco}{req.status_code} {req.reason}{gris}')
        # si la petición no fue correcta, devolvemos el error
        if not req.ok:
            return {"Error" : f"{req.reason}", "status code": f"{req.status_code}"}
        # url del artículo
        d["url"] = req.url
        # creamos el objeto bs4 a partir del código HTML
        soup = BeautifulSoup(req.text, "html.parser")
        # soup = BeautifulSoup(req.text, "html5lib")

        soup.encode('utf-8')
        try:
            #extracción de la información
            fecha = soup.time.text
            titulo = soup.title.text
            d["Fecha"] = fecha
            d["titulo"] = titulo
            d["contenido"] = soup.find("article").text
            with open(f"../data/html/{fecha}_{titulo}.html", 'w') as html_file:
                html_file.write(req.text)

        except Exception:
            logging.exception(f'Failed to scrape url: {url}')

        return d

    def run(self):
        """Crawls URLs, scrapes data from El Financiero articles, and saves results.

        This method performs the following steps:

        1. Crawls up to 10000 URLs from the initial list and any linked URLs found during the process.
        2. Logs crawling activity and any crawl failures.
        3. Filters visited URLs to include only those from El Financiero ("elfinanciero") with a secure HTTPS protocol ("https").
        4. Scrapes data from each El Financiero URL using the `datos_ElFinanciero` method.
        5. Creates a Pandas DataFrame from the scraped data, handling potential encoding issues.
        6. Saves the DataFrame to an Excel file named "finaciero.xlsx" without an index.
        7. Logs any errors encountered during saving.

        Returns:
            list: A list of crawled El Financiero URLs (containing "elfinanciero" and "https").

        Raises:
            Exception: If any unexpected errors occur during crawling or saving.
        """
        i=0
        while self.urls_to_visit:
            url = self.urls_to_visit.pop(0)
            logging.info(f'{i} Crawling: {url}')
            if (i % 200 == 0):
                os.system('clear')
                logging.info(f"Current number of urls to visit: {len(self.urls_to_visit)} in {i} iterations.")
                time.sleep(10)
                if i >= 500:
                    break
            try:
                self.crawl(url)
            except Exception:
                logging.exception(f'Failed to crawl: {url}')
            finally:
                self.visited_urls.append(url)
            i += 1
        try:
            urls = [x for x in self.visited_urls if "elfinanciero" in x and "https" in x]
            rows = []
            for i in urls:
                rows.append(self.datos_ElFinanciero(i))
            df = pd.DataFrame(rows)
            df.columns= df.columns.map(lambda x: x.encode('unicode-escape').decode('latin1'))
            df.to_excel(f"../data/table/finaciero.xlsx", index=False)
        except Exception:
            logging.exception("unable to save content from pages")

        return urls
        

if __name__ == '__main__':
    urls = Crawler(urls=['https://www.elfinanciero.com.mx/']).run()
    print(len(urls))
