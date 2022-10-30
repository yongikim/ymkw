import pprint as pp
from bs4 import BeautifulSoup
import urllib
from urllib.parse import urljoin
import boto3
import json
from typing import List
import csv
from multiprocessing import Pool
from itertools import chain

DOMAIN = 'https://www.furusato-tax.jp'
ROOT_URL = urljoin(DOMAIN, 'search?sort=11')


def fetch_html(url):
    # URLからHTMLを返す
    res = urllib.request.urlopen(url)
    return BeautifulSoup(res, 'html.parser')


# レビューの一覧ページを取得する関数（lambdaを呼び出す関数）
def get_review_pages(url: str):
    client = boto3.client('lambda')
    payload = {
        "url": url
    }
    response = client.invoke(
        FunctionName='urls',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8'),
    )
    decoded = response['Payload'].read().decode('utf-8')
    data = json.loads(decoded)
    body = json.loads(data['body'])
    return body


# 商品一覧ページの最大ページ数を抽出
def max_products_page_num() -> int:
    # HTMLを取得
    soup = fetch_html(ROOT_URL)

    # 最大ページ数を持つ要素を取得
    # <span>15323</span>
    elem = soup.select_one('.nv-pager__item.is-last > span')

    # 最大ページ数が取得できない場合は例外を投げる
    if elem is None or elem.string is None:
        raise RuntimeError("最大ページ数の取得に失敗しました")

    # <span>15323<span> -> 15323
    return int(elem.string)


# 全ての商品一覧ページを生成
def generate_products_urls(max_pages: int) -> List[str]:
    urls = []
    for page in range(max_pages):
        url = ROOT_URL + "&page=" + str(page + 1)
        urls.append(url)

    return urls


if __name__ == "__main__":
    try:
        # 商品一覧ページの最大ページ数を抽出
        max_pages = max_products_page_num()

        # 全ての商品一覧ページを生成
        products_urls = generate_products_urls(max_pages)

        """
        感想一覧ページの [値段, URL, 最大ページ数] のリスト

        - 本番は、products_urls に対してループを回す
        - ここは並列化できる
        """
        # 100ずつ並行処理
        bulk_size = 24
        bulk_num = 1
        for i in range(0, len(products_urls), bulk_size):
            print("bulk", bulk_num)
            urls = products_urls[i:i+bulk_size]

            p = Pool(8)
            result = p.map(get_review_pages, urls)
            result_flat = list(chain.from_iterable(result))

            pp.pprint(result_flat)

            with open('urls.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(result_flat)

                f.close()

            if len(result_flat) == 0:
                break

            bulk_num += 1

    except RuntimeError as e:
        print(e)
