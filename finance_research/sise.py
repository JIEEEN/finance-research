from bs4 import BeautifulSoup
import requests
import ray

@ray.remote
class SiseParser:
    def __init__(self):
        self.sise_url = "https://finance.naver.com/item/sise_day.nhn?code="
        self.headers = {"User-agent": "Mozilla/5.0"}

        self.html = None
        self.bs = None

    def get_last_page(self, ticker: str)-> int:
        self.html = requests.get(self.sise_url + ticker + '&page=1', headers=self.headers).text
        self.bs = BeautifulSoup(self.html, 'lxml')

        pgRR = self.bs.find('td', class_='pgRR')

        last_page = str(pgRR.a['href']).split('=')
        
        return int(last_page[-1])