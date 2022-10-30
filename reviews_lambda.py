import json
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup


def fetch_html(url: str) -> BeautifulSoup:
    html = requests.get(url).text
    return BeautifulSoup(html, 'html.parser')


def get_reviews_per_page(price: int, base_url: str, page: int):
    reviews = []
    url = urljoin(base_url, "?page="+str(page))
    soup = fetch_html(url)
    review_cards = soup.select('.review-list__content')
    for card in review_cards:
        title_elem = card.select_one('.review-list__title')
        title = title_elem.string

        personal = card.select_one('.review-list__data').string
        lm1 = personal.find('｜') + 1
        lm2 = personal.rfind('｜') + 1
        gender = personal[lm1:lm2 - 1]
        age = personal[lm2:]

        date = card.select_one('.review-list__date').string[4:]

        product = card.select_one('.review-list__name').string[3:]

        labels = [elem.string for elem in card.select('.review-tag__text')]
        label = '/'.join(labels)

        text_elem = card.select_one('.review-list__text')
        text = text_elem.decode_contents(formatter="html")
        # 改行の削除
        text = text.replace('\n', '') \
                   .replace('　', ' ') \
                   .replace('<br/>', '')

        reasons = [elem.string for elem in card.select('.review-reason__item')]
        reason = '/'.join(reasons)

        review = [title, gender, age, date,
                  product, price, label, text, reason]
        reviews.append(review)

    return reviews


def get_all_reviews(price: int, url: str,  maxpages: int):
    reviews = []

    for page in range(0, maxpages):
        reviews_per_page = get_reviews_per_page(price, url, page+1)
        reviews += reviews_per_page

    return reviews


def lambda_handler(event, context):
    """
    商品の感想を全て抽出する

    event:
        url      : 感想一覧ページのURL
        price    : 商品価格
        maxpages : 感想一覧ページ数

    returns:
        title   : 感想タイトル
        gender  : 性別
        age     : 年齢
        date    : 日付
        product : 商品名
        price   : 価格
        label   : ラベル
        text    : 本文
        reason  : 商品を選んだ理由
    """

    url = event['url']
    price = int(event['price'])
    maxpages = int(event['maxpages'])

    body = get_all_reviews(price, url, maxpages)

    return {
        'statusCode': 200,
        'body': json.dumps(body).encode('utf-8')
    }
