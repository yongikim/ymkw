import json
from urllib.parse import urljoin
import re
import requests
from bs4 import BeautifulSoup


DOMAIN = 'https://www.furusato-tax.jp'
ROOT_URL = urljoin(DOMAIN, 'search?sort=11')


def get_soup(url: str) -> BeautifulSoup:
    # url から BeautifulSoup を生成
    html = requests.get(url).text
    return BeautifulSoup(html, 'html.parser')


def find_reviews_urls(products_url: str) -> list[str]:
    """
    1つの商品一覧ページから、全ての商品と感想数を取得

    returns:

        [[商品Aの名前, 感想数],
         [商品Bの名前, 感想数],
         [商品Cの名前, 感想数],
         [商品Dの名前, 感想数],
         ... ]

    """

    # 商品一覧ページのHTMLを取得
    soup = get_soup(products_url)

    # 商品カード
    product_cards = soup.select('.card-product')

    # list of [商品名, 感想数]
    result = []
    for card in product_cards:
        # 商品名の取得
        url_elem = card.select_one('.card-product__comment')
        if url_elem is None:
            continue
        href = url_elem.get('href')
        if href is None:
            continue
        url = urljoin(DOMAIN, href)
        review_page_html = get_soup(url)
        title_elem = review_page_html.select_one(
            '.review-info-header__product-text')
        if title_elem is None:
            continue
        title = title_elem.string
        if title is None:
            continue

        # '感想(211)'
        kansou_str = list(url_elem.children)[2]

        # カッコの中を正規表現で抽出
        kansou_num_str = re.search(r'(?<=\().*(?=\))', kansou_str).group()
        kansou_num = int(kansou_num_str)

        # [商品名, 感想数]
        result.append([title, kansou_num])

    return result


def lambda_handler(event, context):
    url = event["url"]
    body = find_reviews_urls(url)
    return {
        'statusCode': 200,
        'body': json.dumps(body)
    }
