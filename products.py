from bs4 import BeautifulSoup
import urllib
from urllib.parse import urljoin
import boto3
from botocore.exceptions import ClientError
import json
from typing import List
import csv
from multiprocessing import Pool
from itertools import chain
import math
import time

DOMAIN = 'https://www.furusato-tax.jp'
ROOT_URL = urljoin(DOMAIN, 'search?sort=11')


# URLからHTMLを返す
def fetch_html(url):
    res = urllib.request.urlopen(url)
    return BeautifulSoup(res, 'html.parser')


# Lambda を呼び出す
def invoke_lambda(fun: str, payload: object):
    client = boto3.client('lambda')
    response = client.invoke(
        FunctionName=fun,
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8'),
    )
    return response['Payload'].read().decode('utf-8')


def get_products(url: str, retry=0):
    """
    商品の情報をリスト形式で取得
    失敗した場合は5秒休んでリトライする
    5回リトライしてだめなら，htmlを保存して諦める

       [[商品名, 値段，感想数],
        [商品名, 値段，感想数], ...]

    """

    if retry >= 5:
        # TODO: save html
        print('RETRY LIMIT SAVING HTML', url)
        html = fetch_html(url)
        with open(url.replace('/', '_').replace(':', '_') + '.txt', 'w') as f:
            f.write(str(html))

        return []

    fun_name = 'products'
    payload = {
        "url": url,
    }

    try:
        result = invoke_lambda(fun_name, payload)
        data = json.loads(result)

        if 'body' in data:
            return json.loads(data['body'])

        elif 'error' in data:
            print('RETRY', data['url'], data['error'])
            time.sleep(5)

            return get_products(url, retry=retry+1)

        else:
            print('RETRY Unexpected', data, url)
            time.sleep(5)

            return get_products(url, retry=retry+1)

    except ClientError as e:
        print('RETRY ClientError', url, e)
        time.sleep(5)

        return get_products(url, retry=retry+1)


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
        print(str(max_pages) + ' pages found.')

        # 全ての商品一覧ページを生成
        products_urls = generate_products_urls(max_pages)

        """
        感想一覧ページの [値段, URL, 最大ページ数] のリスト

        - 本番は、products_urls に対してループを回す
        - ここは並列化できる
        """
        # 8個ずつ並行処理
        bulk_size = 8
        bulk_num = 1
        bulk_end = math.ceil(max_pages / bulk_size)
        print('bulk end', bulk_end, '\n')
        for i in range(0, len(products_urls), bulk_size):
            print("bulk", bulk_num)
            urls = products_urls[i:i+bulk_size]

            p = Pool(8)
            result = p.map(get_products, urls)
            result_flat = list(chain.from_iterable(result))

            with open('products_reviews.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(result_flat)

                f.close()

            bulk_num += 1

    except RuntimeError as e:
        print('error', e)
