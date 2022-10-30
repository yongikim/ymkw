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

    # 商品カードページ (31番目以降は下部の「最近見たお礼の品」なので除外)
    product_cards = soup.select('div[class="card-product"]')[:30]

    # list of [商品名, 感想数]
    result = []
    for card in product_cards:

        # 商品名の取得
        product_link_elem = card.select_one('.card-product__link')

        href = ''
        if product_link_elem:
            href = product_link_elem.get('href')

        title = 'no title'
        if href is None or len(href) == 0:
            href = products_url
        else:
            link = urljoin(DOMAIN, href)
            product_page_html = get_soup(link)
            title_elem = product_page_html.select_one('.ttl-h1__text')
            if title_elem:
                title = title_elem.string
                if title is None:
                    strings = list(title_elem.stripped_strings)
                    if len(strings) == 0:
                        title = 'no title'
                    else:
                        # 「チョイス限定」に対応
                        title = strings[-1]
                if title is None:
                    title = 'no title'

        title = title.replace('\n', '') \
                     .replace('<br/>', '')

        # 値段の取得
        price = 0
        price_elem = card.select_one('.card-product__price')
        if price_elem:
            price_str = price_elem.string.replace(u'\xa0', ' ').split()[0]
            price = int(price_str.replace(',', ''))

        # '感想(211)'
        url_elem = card.select_one('.card-product__comment')
        if url_elem is None:
            # [商品名, 値段, 感想数, URL]
            result.append([title, price, 0, href])
            continue

        kansou_children = list(url_elem.children)
        kansou_str = kansou_children[2]

        # カッコの中を正規表現で抽出
        kansou_num_str = re.search(r'(?<=\().*(?=\))', kansou_str).group()
        kansou_num = int(kansou_num_str)

        # [商品名, 値段, 感想数, URL]
        result.append([title, price, kansou_num, href])

    return result


def lambda_handler(event, context):
    url = event["url"]
    response = {'statusCode': 200}

    try:
        body = find_reviews_urls(url)
        response['body'] = json.dumps(body)
    except Exception as e:
        print(e)
        response['error'] = json.dumps(str(e))
    finally:
        response['url'] = url

    return response
