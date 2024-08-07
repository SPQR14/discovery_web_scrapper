import logging
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
import pandas as pd

azul = "\33[1;36m"
gris = "\33[0;37m"
blanco = "\33[1;37m"

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(message)s',
    level=logging.INFO)

class Crawler:

    def __init__(self, urls=[]):
        self.visited_urls = []
        self.urls_to_visit = urls

    def download_url(self, url):
        return requests.get(url).text

    def get_linked_urls(self, url, html):
        soup = BeautifulSoup(html, 'html.parser')
        for link in soup.find_all('a'):
            path = link.get('href')
            if path and path.startswith('/'):
                path = urljoin(url, path)
            yield path

    def add_url_to_visit(self, url):
        if url not in self.visited_urls and url not in self.urls_to_visit:
            self.urls_to_visit.append(url)

    def crawl(self, url):
        html = self.download_url(url)
        for url in self.get_linked_urls(url, html):
            self.add_url_to_visit(url)

    def datos_ElFinanciero(self, url):
        """
        Args:
            url (String): URL of the element to extract the information
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
        # url del producto
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
            with open(f"{fecha}_{titulo}.html", 'w') as html_file:
                html_file.write(req.text)

        except Exception:
            logging.exception(f'Failed to scrape url: {url}')

        return d

    def run(self):
        i=0
        while i < 500:
            url = self.urls_to_visit.pop(0)
            logging.info(f'Crawling: {url}')
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
            df.to_excel(f"finaciero.xlsx", index=False)
        except Exception:
            logging.exception("unable to save content from pages")
        finally:
            return urls

if __name__ == '__main__':
    urls = Crawler(urls=['https://www.elfinanciero.com.mx/']).run()
    urls = [x for x in urls if "elfinanciero" in x]
    print(len(urls))
