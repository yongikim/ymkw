from urllib.parse import urljoin
import boto3
import json
import csv
from multiprocessing import Pool
from itertools import chain

DOMAIN = 'https://www.furusato-tax.jp'
ROOT_URL = urljoin(DOMAIN, 'search?sort=11')


# 全ての感想を取得（lambdaを呼び出す関数）
def get_reviews(price: int, url: str, maxpages: int):
    client = boto3.client('lambda')
    payload = {
        "price": price,
        "url": url,
        "maxpages": maxpages
    }
    response = client.invoke(
        FunctionName='reviews',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload).encode('utf-8'),
    )
    decoded = response['Payload'].read().decode('utf-8')
    data = json.loads(decoded)
    body = json.loads(data['body'])

    return body


if __name__ == "__main__":
    try:
        # CSVから商品価格, URL, 感想ページ数を順に読み込む (row)
        data = []
        with open('urls.csv', 'r') as f:
            reader = csv.reader(f, delimiter=',')
            for row in reader:
                data_row = [int(row[0]), row[1], int(row[2])]
                data.append(data_row)

        # 全ての row に対して、全ての感想ページに対して
        # すべての感想データを抽出する
        # lambdaの中で感想ページをなめる
        # 8ずつ並行処理
        bulk_size = 8
        bulk_num = 1
        for i in range(0, len(data), bulk_size):
            print("bulk", bulk_num)
            rows = data[i:i+bulk_size]

            p = Pool(8)

            """
            lambdaでその商品の感想を全て抽出する

            - title   : 感想タイトル
            - gender  : 性別
            - age     : 年齢
            - date    : 日付
            - product : 商品名
            - price   : 価格
            - label   : ラベル
            - text    : 本文
            - reason  : 商品を選んだ理由
            """
            args = [(int(row[0]), row[1], int(row[2])) for row in rows]
            result = p.starmap(get_reviews, args)
            result_flat = list(chain.from_iterable(result))

            # CSVに保存する
            with open('reviews.csv', 'a', newline='') as f:
                writer = csv.writer(f)
                writer.writerows(result_flat)

                f.close()

            bulk_num += 1

    except RuntimeError as e:
        print(e)
