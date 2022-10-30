import json
import urllib
from typing import List
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import math


DOMAIN = 'https://www.furusato-tax.jp'
ROOT_URL = urljoin(DOMAIN, 'search?sort=11')


def fetch_html(url):
    # URLからHTMLを返す
    res = urllib.request.urlopen(url)
    return BeautifulSoup(res, 'html.parser')


def get_ip_addr():
    # 現在のグローバルIPアドレスを返す
    html = fetch_html('http://checkip.dyndns.com/')
    return html.body.text.split(': ')[1]


def find_reviews_urls(products_url: str) -> List[str]:
    """
    1つの商品一覧ページから、全ての感想一覧ページを取得
    商品の値段は感想一覧ページにはないので、ここで取得しておく

    1. 感想一覧ページのURLを取得
    2. 例えば 234 / 10 = 23 なので、24 ページまでとわかる
    3. [商品の値段, 1 のURL, 2 のページ数] を保存する

    returns:

        [[商品Aの値段
         [商品Bの値段, 感想一覧ページのURL, 最大ページ数],
         [商品Cの値段, 感想一覧ページのURL, 最大ページ数],
         [商品Dの値段, 感想一覧ページのURL, 最大ページ数],
         ... ]

    """

    # 商品一覧ページのHTMLを取得
    soup = fetch_html(products_url)

    # 商品カード
    product_cards = soup.select('.card-product')

    # list of [商品の値段, 感想一覧ページのURL, 最大ページ数]
    price_url_maxpages = []
    for card in product_cards:
        price_elem = card.select_one('.card-product__price')
        url_elem = card.select_one('.card-product__comment')

        # 存在しない場合がある
        if price_elem is None or url_elem is None:
            continue

        # href属性の取得
        href = url_elem.get('href')

        # 存在しない場合がある
        if href is None:
            continue

        # 相対URL -> 絶対URL
        url = urljoin(DOMAIN, href)

        # 感想ページ数の取得
        # '感想(211)' -> 211
        kansou_str = list(url_elem.children)[2]
        # カッコの中を正規表現で抽出
        max_pages_str = re.search(r'(?<=\().*(?=\))', kansou_str).group()
        # 10 で割って繰り上げ
        max_pages = math.ceil(int(max_pages_str) / 10)

        # '10,000\xa0円' -> '10,000 円' -> '10,000'
        price_str = price_elem.string.replace(u'\xa0', ' ').split()[0]
        # '10,000' -> 10000
        price = int(price_str.replace(',', ''))

        # [商品の値段, 感想一覧ページのURL, 最大ページ数]
        price_url_maxpages.append([price, url, max_pages])

    return price_url_maxpages


def lambda_handler(event, context):
    # ip = get_ip_addr()
    # print(ip)
    url = event["url"]
    body = find_reviews_urls(url)
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps(body)
    }
