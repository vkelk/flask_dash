import requests
import re
import json
import time
from lxml import html, etree
from w3lib.html import remove_tags
import random
import settings
from sqlalchemy import (create_engine, MetaData, Column, BIGINT, VARCHAR, INTEGER, TEXT, ForeignKey,
                        DATE, TIME)
from sqlalchemy import event, exc
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import sqlalchemy.exc
import multiprocessing
from multiprocessing import Process
import csv
import os
import traceback

pg_config = {
    'username': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'database': settings.PG_DBNAME,
    'host': settings.DB_HOST
}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}:5432/{database}".format(**pg_config)


def add_engine_pidguard(engine):
    @event.listens_for(engine, "connect")
    def connect(dbapi_connection, connection_record):
        connection_record.info['pid'] = os.getpid()

    @event.listens_for(engine, "checkout")
    def checkout(dbapi_connection, connection_record, connection_proxy):
        pid = os.getpid()
        if connection_record.info['pid'] != pid:
            # substitute log.debug() or similar here as desired
            warnings.warn("Parent process %(orig)s forked (%(newproc)s) with an open "
                          "database connection, "
                          "which is being discarded and recreated." % {
                              "newproc": pid,
                              "orig": connection_record.info['pid']
                          })
            connection_record.connection = connection_proxy.connection = None
            raise exc.DisconnectionError("Connection record belongs to pid %s, "
                                         "attempting to check out in pid %s" %
                                         (connection_record.info['pid'], pid))


Base = declarative_base()
db_engine = create_engine(pg_dsn)
add_engine_pidguard(db_engine)
pg_meta = MetaData(bind=db_engine, schema="seekingalpha")
Base.metadata.create_all(db_engine)
Session = sessionmaker(bind=db_engine, autoflush=False)
ScopedSession = scoped_session(sessionmaker(bind=db_engine, autoflush=False))
session = Session()


class Author(Base):
    __tablename__ = 'author'
    author_id = Column(BIGINT, primary_key=True)
    author_name = Column(VARCHAR(255))
    author_url = Column(VARCHAR(2048))
    member_since = Column(INTEGER)
    contributor_since = Column(INTEGER)
    strategy = Column(VARCHAR(255))
    intro = Column(TEXT)
    articles_written = Column(INTEGER)
    comments_posted = Column(INTEGER)
    stocktalks_count = Column(INTEGER)
    instablogs_count = Column(INTEGER)
    followers_count = Column(INTEGER)
    following_count = Column(INTEGER)
    company_name = Column(VARCHAR(255))
    author_website = Column(VARCHAR(2048))
    twitter_id = Column(BIGINT)
    author_slug = Column(VARCHAR(255))

    articles = relationship('Article')
    blogposts = relationship('Blogposts')
    stocktalks = relationship('Stocktalks')
    comments = relationship('Comment')


class Article(Base):
    __tablename__ = 'article'
    article_id = Column(BIGINT, primary_key=True)
    article_url = Column(VARCHAR(2048))
    author_id = Column(BIGINT, ForeignKey('author.author_id'))
    article_title = Column(TEXT)
    published_date = Column(DATE)
    published_time = Column(TIME(6))
    disclosure = Column(TEXT)
    article_summary = Column(TEXT)
    article_text = Column(TEXT)
    comments_received = Column(INTEGER)
    article_likes = Column(INTEGER)
    article_timezone = Column(VARCHAR(15))

    article_urls = relationship('Article_urls')
    article_tickers = relationship('Article_tickers')
    comments = relationship('Comment')


class Article_urls(Base):
    __tablename__ = 'article_urls'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    article_id = Column(BIGINT, ForeignKey('article.article_id'))
    url = Column(VARCHAR(2048))


class Article_tickers(Base):
    __tablename__ = 'article_tickers'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    article_id = Column(BIGINT, ForeignKey('article.article_id'))
    ticker = Column(VARCHAR(15))
    type = Column(VARCHAR(15))


class Blogposts(Base):
    __tablename__ = 'blogposts'
    blog_id = Column(BIGINT, primary_key=True)
    blog_name = Column(VARCHAR(255))
    blogpost_id = Column(VARCHAR(255))
    blogpost_title = Column(VARCHAR(255))
    publish_date = Column(DATE)
    publish_time = Column(TIME(6))
    blog_timezone = Column(VARCHAR(15))
    blogpost_includes = Column(VARCHAR(255))
    blogpost_comments = Column(INTEGER)
    blogpost_replies = Column(INTEGER)
    blogpost_url = Column(VARCHAR(2048))
    blogpost_links = Column(VARCHAR(2048))
    author_id = Column(BIGINT, ForeignKey('author.author_id'))


class Stocktalks(Base):
    __tablename__ = 'stocktalks'
    stocktalk_id = Column(BIGINT, primary_key=True)
    author_id = Column(BIGINT, ForeignKey('author.author_id'))
    publish_date = Column(DATE)
    publish_time = Column(TIME(6))
    stocktalk_timezone = Column(VARCHAR(15))
    stocktalk_likes = Column(INTEGER)
    stocktalk_text = Column(TEXT)
    stocktalk_url = Column(VARCHAR(2048))

    stocktalk_urls = relationship('Stocktalk_urls')


class Stocktalk_urls(Base):
    __tablename__ = 'stocktalk_urls'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    stocktalk_id = Column(BIGINT, ForeignKey('stocktalks.stocktalk_id'))
    stocktalk_url = Column(VARCHAR(2048))


class Comment(Base):
    __tablename__ = 'comment'
    comment_id = Column(BIGINT, primary_key=True)
    article_id = Column(BIGINT, ForeignKey('article.article_id'))
    author_id = Column(BIGINT, ForeignKey('author.author_id'))
    publish_date = Column(DATE)
    publish_time = Column(TIME(6))
    comment_likes = Column(INTEGER)
    comments_text = Column(TEXT)
    comment_url = Column(VARCHAR(2048))
    comment_timezone = Column(VARCHAR(15))
    comment_type = Column(VARCHAR(15))
    comment_parent_id = Column(BIGINT)

    comment_urls = relationship('Comment_urls')


class Comment_urls(Base):
    __tablename__ = 'comment_urls'
    id = Column(BIGINT, primary_key=True, autoincrement=True)
    comment_id = Column(BIGINT, ForeignKey('comment.comment_id'))
    comment_url = Column(VARCHAR(2048))


class LoadingError(Exception):
    pass


class Page(object):
    def __init__(self, proxy=None, n=None):
        self.n = n
        with open('ua.txt') as f:
            l = f.readlines()
        i = random.randrange(0, len(l))
        self.pr = {'http': proxy, 'https': proxy}
        self.timeout = 30
        self.ses = requests.Session()
        self.ses.headers = {
            'user-agent': l[i].strip(' ').strip('\n'),
            'Connection': 'keep-alive',
        # 'Cache-Control': 'max-age=0',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/javascript, text/html, application/xml, text/xml, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Host': 'seekingalpha.com',
            'Referer': 'https://seekingalpha.com/',
            'Origin': 'https://seekingalpha.com',
            'dnt': '1'
        }

    def __del__(self):
        self.ses.close()

    def close(self):
        self.ses.close()

    def load(self, method, url, params=None, headers=None, important=True):
        error_count = 0
        while True:
            try:
                if method == 'get':
                    resp = self.ses.get(
                        url, proxies=self.pr, params=params, headers=headers, timeout=self.timeout)
                elif method == 'post':
                    resp = self.ses.post(
                        url, proxies=self.pr, data=params, headers=headers, timeout=self.timeout)
            except requests.exceptions.RequestException as e:
                error_count += 1
                # print('Loading error', error_count, pr, e)
                if error_count < 3:
                    print(self.n, 'Loading error', error_count, url, self.pr, e)
                    time.sleep(60)
                    continue
                print(self.n, 'Error limit exceeded Loading error', url, self.pr, e)
                if important:
                    raise LoadingError
                else:
                    return None

            if resp.status_code == requests.codes.ok:
                # time.sleep(1)
                return resp.text
            elif resp.status_code == 403:
                res = re.findall('<div class="g-recaptcha" data-sitekey="(.+?)"', resp.text, re.S)
                if res:
                    print(self.n, 'Captcha', res[0])

                print(
                    self.n,
                    'Error 403',
                    method,
                    url,
                    params,
                )
                if important:
                    raise LoadingError
                else:
                    return None
            elif resp.status_code == 404:
                print(self.n, 'Error 404', method, url, params, resp.text, resp.status_code)
                if important:
                    raise LoadingError
                else:
                    return None
            elif resp.status_code == 429:
                print('{} Rate limit Sleep 3 min'.format(self.n))
                time.sleep(3 * 60)
                continue
            elif resp.status_code == 503:
                # print('503 Error waiting 2 min', screen_name, proxy, url, resp.text, resp.status_code)
                error_count += 1
                if error_count > 5:
                    print('AWAM: Requestes 503 error', url, self.pr)
                    if important:
                        raise LoadingError
                    else:
                        return None

                print(' Requestes 503 error', error_count, url, self.pr)
                time.sleep(120)
                continue
            else:
                print(self.n, 'Error', url, resp.text, resp.status_code)
                error_count += 1
                print(self.n, 'Loading error', error_count)
                if error_count > 5:
                    print(self.n, 'Error limit exceeded Requestes error ', url, self.pr,
                          resp.status_code)
                    if important:
                        raise LoadingError
                    else:
                        return None
                print(self.n, 'Error waiting 1 min', url, resp.text, resp.status_code)
                time.sleep(60)
                continue


def get_page(ses, url):
    error_count = 0
    while True:
        try:
            resp = ses.get(url, timeout=10)
            r = json.loads(resp.text)
        except (requests.exceptions.RequestException, json.decoder.JSONDecodeError) as e:
            error_count += 1
            if error_count > 2:
                print('Problem with connection ', url, e)
                exit(-1)
            time.sleep(1)
            continue
        if resp.status_code == requests.codes.ok:
            return r

    print('Error', url, resp.status.code, r)
    exit(-1)


def cleanhtml(raw_html):
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', raw_html)
    return cleantext
    return remove_tags(raw_html)


class SeekingAlpha(object):
    def __init__(self, login, password, proxy=None, logname=None, n=None):
        self.page = Page(proxy, n)
        self.login(login, password)
        self.n = n

    def login(
            self,
            login,
            password,
    ):
        r = self.page.load('get', 'https://seekingalpha.com/')
        h = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'X-Compress': 'null',
            'X-Requested-With': 'XMLHttpRequest'
        }
        # p={'user[email]':'tek.advant@gmail.com','user[password]':'tekadvant','id':'headtabs_login',
        #     'activity':'roadblock','function':'onLoginSuccess'}
        p = {
            'user[email]': login,
            'user[password]': password,
            'id': 'headtabs_login',
            'activity': 'footer_login',
            'function': 'onLoginSuccess'
        }

        response = self.page.load(
            'post', 'https://seekingalpha.com/authentication/login', headers=h, params=p)

        r = json.loads(response)
        if r['login']:
            return r['login']
        else:
            print('Unable login{}'.format(''))

    def load_article_urls_by_userid(self, slug, userid):
        count = 0
        article_urls = []
        page = 0
        while True:
            url = 'https://seekingalpha.com/author/' + str(
                slug) + '/ajax_load_regular_articles?page=' + str(
                    page) + '&author=true&userId=' + str(userid) + '&sort=recent'

            response = self.page.load('get', url)

            if response:
                j = json.loads(response)
                html_content = j['html_content']
                tree = html.fromstring(html_content)
                divs = tree.xpath('//div[@data-behavior="article-container"]')
                for d in divs:
                    if not d.xpath('.//span[@class="archive-lock "]'):
                        u = d.xpath('.//a/@href')[0]
                        u = re.sub('\?.+?$', '', u)
                        article_urls.append('https://seekingalpha.com' + u)
                        count += 1
                        # if count > 3:
                        #     return article_urls

            page += 1
            if page == int(j['page_count']):
                break
            time.sleep(1)
        return article_urls

    def get_author_by_id(self, author_id):
        author = {}

        url = 'http://seekingalpha.com/user/' + str(author_id)
        response = self.page.load('get', url)

        if response:
            res = re.findall('SA\.Pages\.Profile\.init\((.+?)\);', response, re.S)

            r = json.loads(res[0])
            # user_id = r['profile_id']

            # print(json.dumps(r, indent=4))
            tree = html.fromstring(response)
            t = tree.xpath('//div[@data-info="bio"]/p')
            if t:
                bio = t[0].text_content()
                author['intro'] = re.sub('\s+$', '', bio)
            else:
                author['intro'] = None

            author['author_name'] = r['profile_info']['name']
            author['author_url'] = url
            author['member_since'] = r['profile_info']['member_since']
            res = re.findall('Contributor since:</span><span> (\d+)</span>', response, re.S)
            author['contributor_since'] = res[0] if res else None
            author['strategy'] = None
            author['instablogs_count'] = r['object_count']['instablogs_count'] if r[
                'object_count'].get('instablogs_count', False) else 0
            author['articles_written'] = r['object_count']['articles_count'] if r[
                'object_count'].get('articles_count', False) else 0
            author['comments_posted'] = r['object_count']['comments_count'] if r[
                'object_count'].get('comments_count', False) else 0
            author['stocktalks'] = r['object_count']['stocktalks_count'] if r['object_count'].get(
                'stocktalks_count', False) else 0
            author['followers_count'] = r['object_count']['followers_count'] if r[
                'object_count'].get('followers_count', False) else 0
            author['following_count'] = r['object_count']['following_count'] if r[
                'object_count'].get('following_count', False) else 0

            res = re.findall(
                'Company: </span><span data-info="company-name" itemprop="worksFor">(.+?)</span></div><ul><li><a',
                response, re.S)

            author['company_name'] = res[0] if res else None

            res = re.findall('id="personal_url" href="(.+?)"', response, re.S)
            author['author_website'] = res[0] if res and res[0] != '#' else None

            author['twitter_id'] = None
            res = re.findall('id="twitter" href="(.+?)"', response, re.S)
            tweet_url = res[0] if res and res[0] != '#' else None
            # h = {'Host': '',
            #      'Referer': '',
            #      'Origin': '',
            #      'dnt': ''}
            if tweet_url:
                tweet_resp = requests.get(tweet_url, timeout=10)
                if tweet_resp.status_code == requests.codes.ok:
                    res = re.findall('data-user-id="(\d+)">', tweet_resp.text, re.S)
                    if res:
                        author['twitter_id'] = int(res[0])
            author['author_slug'] = r['author_specific_info']['slug'] if r.get(
                'author_specific_info', False) else None

            return author
        return None

    def get_img_url(self, raw_html):
        res = re.findall('<img src="(.+?)"', raw_html, re.S)
        return res

    def check_stocktals(self, st):
        stocktalk = {}
        data_id = st.xpath('@data-id')
        data_user_id = st.xpath('@data-user-id')
        data_uri = st.xpath('@data-uri')
        publish_date = st.xpath('.//div/div[@class="c-stocktalk__time"]/text()')[0]
        stocktalk_text = st.xpath('./div/div/div[@class="c-stocktalk__text"]')
        stocktalk_text = cleanhtml(
            etree.tostring(stocktalk_text[0], method='html', with_tail=False).decode())
        stocktalk['stocktalk_id'] = int(data_id[0])
        stocktalk['author_id'] = int(data_user_id[0])
        stocktalk['publish_date'] = str(publish_date)
        stocktalk['publish_time'] = None
        stocktalk['stocktalk_timezone'] = None
        stocktalk['stocktalk_likes'] = None
        stocktalk['stocktalk_text'] = stocktalk_text
        stocktalk['stocktalk_url'] = str(data_uri[0])

        return stocktalk

    def get_stocktalks_by_userid(self, slug, userid):
        count = 0
        stocktalks = []
        page = 0
        while True:
            url = 'https://seekingalpha.com/user/' + str(
                userid) + '/ajax_load_stocktalks?page=' + str(page) + '&author=false&userId=' + str(
                    userid) + '&sort=recent'

            response = self.page.load('get', url)
            # h = {'accept': '*/*', 'accept-encoding': 'gzip, deflate, br',
            #      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            #      'x-requested-with': 'XMLHttpRequest','referer':'referer: https://seekingalpha.com/author/'+slug+'/stocktalks'}
            # self.page.load('post', 'https://seekingalpha.com/mone_event',
            #                headers=h,
            #                params={'version': 2, 'key': 'jfeieqst', 'type': 'profile', 'source': 'stocktalks',
            #                        'action': 'load_more', 'data': {}})
            if response:
                r = json.loads(response)
                tree = html.fromstring(r['html_content'])
                csto = tree.xpath('//div[@class="c-stocktalk c-stocktalk__container"]')
                for st in csto:
                    stocktalks.append(self.check_stocktals(st))
                # c_thread = tree.xpath('.//div[@class="c-stocktalk__sa-btn js-stocktalk-view-thread"]/@st_id')
                # for c_t in c_thread:
                #     time.sleep(1)
                #     h = {'accept': '*/*', 'accept-encoding': 'gzip, deflate, br',
                #          'content-type': 'application/x-www-form-urlencoded; charset=UTF-8','dnt':'1','accept-language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
                #          'x-requested-with': 'XMLHttpRequest','referer':'https://seekingalpha.com/author/'+slug+'/stocktalks','origin': 'https://seekingalpha.com'}
                #     response = self.page.load('post', 'https://seekingalpha.com/stocktalks_jq/ajax_get_replies_jq',
                #                               headers=h, params={'id': int(c_t)})
                #
                #     r2 = json.loads(response)
                #     if r2['success']:
                #         tree = html.fromstring(r2['html'])
                #         csto = tree.xpath('//div[@class="c-stocktalk c-stocktalk__container"]')
                #         for st in csto:
                #             stocktalks.append(self.check_stocktals(st))
                #             i = 1
            page += 1
            # print(page, len(stocktalks))
            # if not page %10:
            #     self.page.load('get','https://seekingalpha.com/')
            #     time.sleep(30)
            if page == int(r['page_count']):
                break
            time.sleep(1)

        i = len(stocktalks)
        return stocktalks

    def get_article_by_url(self, url):
        response = self.page.load('get', url)

        if response:
            res = re.findall('id="liftigniter-metadata" type="application/json">(.+?)</script>',
                             response, re.S)
            res = re.findall('<script>window.SA = (.+?);</script>', response, re.S)
            # r = json.loads(res[0])
            # res = re.findall('w.SA = (.+?);</script>', response, re.S)
            if res:
                article = {}
                r = json.loads(res[0])
                try:
                    article_dirty = r['pageConfig']['Data']['article']
                except:
                    print(json.dumps(r['pageConfig']['Data'], indent=4))
                    exit(0)
                if article_dirty['isProPaywall']:
                    print('   isProPaywall')
                    return {}
                res = re.findall(
                    '<div class="sa-art article-width" id="a-body" itemprop="articleBody">(.+?)</div><div id="article-options">',
                    response, re.S)
                if res:
                    article['media_urls'] = self.get_img_url(res[0])
                    article['article_text'] = cleanhtml(res[0])
                else:
                    article['media_urls'] = None
                    article['article_text'] = None

                res = re.findall(
                    '<p id="a-disclosure" class=".+?">(.+?)</div><div id="article-options">',
                    response, re.S)
                if res:
                    article['disclosure'] = cleanhtml(res[0])
                else:
                    article['disclosure'] = None

                res = re.findall('<div class="a-sum" itemprop="description">(.+?)</div>', response,
                                 re.S)
                if res:
                    article['article_summary'] = cleanhtml(res[0])
                else:
                    article['article_summary'] = None
                try:
                    article['about_ticker'] = r['pageConfig']['Ads']['kvs']['pr'] if isinstance(
                        r['pageConfig']['Ads']['kvs']['pr'],
                        list) else [r['pageConfig']['Ads']['kvs']['pr']]
                except:
                    article['about_ticker'] = []
                try:
                    article['include_tickers'] = article['include_tickers'] + (
                        r['pageConfig']['Ads']['kvs']['s']
                        if isinstance(r['pageConfig']['Ads']['kvs']['s'],
                                      list) else [r['pageConfig']['Ads']['kvs']['s']])
                    if article['about_ticker']:
                        article['include_tickers'].append(article['about_ticker'])
                except:
                    article['include_tickers'] = []
                article['article_id'] = int(article_dirty['id'])
                article['article_url'] = url
                article['article_title'] = article_dirty['title']
                article['published_date'] = article_dirty['article_datetime'][0:10]
                article['published_time'] = article_dirty['article_datetime'][11:19]
                article['article_timezone'] = article_dirty['article_datetime'][23:]
                article['author_id'] = r['pageConfig']['Data']['author']['userId']
                article['author_slug'] = r['pageConfig']['Data']['author']['slug']

                article_dirty['author'] = r['pageConfig']['Data']['author']
                h = {
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'x-requested-with': 'XMLHttpRequest',
                    'dnt': '1',
                    'referer': url
                }
                count_error = 0
                while True:
                    comment_r = self.page.load(
                        'get',
                        'https://seekingalpha.com/account/ajax_get_comments?id=' +
                        str(article['article_id']) + '&type=Article',
                        headers=h,
                        important=False)
                    if comment_r or count_error >= 3:
                        break
                    count_error += 1

                    time.sleep(5)
                    # print('attempt', count_error)
                    # self.page.load('get', url)

                article['comments_received'] = 0
                article['comments'] = []
                if comment_r:
                    r_c = json.loads(comment_r)
                    if r_c['status'] == 'success':
                        article['comments_received'] = int(r_c['total'])
                        if article['comments_received']:
                            article['comments'] = r_c['comments']
                        else:
                            article['comments'] = []

                likers_r = self.page.load('get', 'https://seekingalpha.com/likes/likers?id=' + str(
                    article['article_id']) + '&type=Article&variation=test_3')
                r_l = json.loads(likers_r)
                if r_l['success']:
                    article['article_likes'] = len(r_l['user_ids'])
                else:
                    article['article_likes'] = 0

                return article
        return None

    def get_instablogurl_by_userid(self, userid):
        count = 0
        insta_blogs = []
        page = 0
        while True:
            'https://seekingalpha.com/author/sa-eli-hoffmann/ajax_load_instablogs?page=0&author=true&userId=6383&sort=recent'
            url = 'https://seekingalpha.com/user/' + str(
                userid) + '/ajax_load_instablogs?page=' + str(page) + '&author=true&userId=' + str(
                    userid) + '&sort=recent'

            response = self.page.load('get', url)

            if response:
                j = json.loads(response)
                html_content = j['html_content']
                tree = html.fromstring(html_content)
                divs = tree.xpath('//div[@data-behavior="article-container"]')
                for d in divs:
                    info = etree.tostring(d).decode('utf-8')
                    res = re.findall('<a href="/instablog/(.+?)">(.+?)</a>', info)
                    insta_blog = 'https://seekingalpha.com/instablog/' + res[0][0]
                    insta_blog = re.sub('\?.+?$', '', insta_blog)
                    # insta_blog['title'] = res[0][1]
                    insta_blogs.append(insta_blog)

            page += 1
            if page == int(j['page_count']):
                break
            time.sleep(1)
        return insta_blogs

    def get_instablog_by_userid(self, userid):
        instablogs = []
        url_list = self.get_instablogurl_by_userid(userid)
        for url in url_list:
            response = self.page.load('get', url)
            res = re.findall('<script>window.SA = (.+?);</script>', response, re.S)
            if res:
                instablog = {}
                r = json.loads(res[0])
                instablog['blog_id'] = r['pageConfig']['Data']['instablog']['id']
                res = re.findall('<a class="nick" href=".+?">(.+?)</a>', response, re.S)
                instablog['blog_name'] = res[0]
                instablog['blogpost_id'] = r['pageConfig']['Data']['instablog']['id']
                instablog['blogpost_title'] = r['pageConfig']['Data']['instablog']['title']
                res = re.findall('<time datetime="(.+?) (.+?) (.+?)"', response, re.S)
                instablog['publish_date'] = res[0][0]
                instablog['publish_time'] = res[0][1]
                instablog['blog_timezone'] = res[0][2]
                tree = html.fromstring(response)
                divs = tree.xpath('//a[@sasource="instablog_about"]/@href')

                instablog['blogpost_includes'] = ' '.join(
                    [re.sub('/symbol/', '', d) for d in divs])[:255]
                instablog['comments'] = 0
                h = {
                    'accept': '*/*',
                    'accept-encoding': 'gzip, deflate, br',
                    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'x-requested-with': 'XMLHttpRequest',
                    'dnt': '1',
                    'referer': url
                }
                comment_r = self.page.load(
                    'get',
                    'https://seekingalpha.com/account/ajax_get_comments?id=' +
                    str(instablog['blog_id']) + '&type=InstablogPost',
                    headers=h,
                    important=None)
                if comment_r:
                    j = json.loads(comment_r)
                    if j['status'] == 'success':
                        instablog['comments'] = int(j['total'])

                instablog['blogpost_replies'] = None
                instablog['blogpost_url'] = url
                instablog['blogpost_links'] = None
                instablog['author_id'] = r['pageConfig']['Data']['author']['userId']
                instablogs.append(instablog)
                time.sleep(1)
        return instablogs

    def get_comments_by_userid(self, userid):
        count = 0
        stocktalks = []
        page = 0
        while True:
            url = 'https://seekingalpha.com/user/' + str(userid) + '/ajax_load_comments?page=' + str(
                page) + '&author=false&userId=' + str(userid) + '&sort=recent'

            response = self.page.load('get', url)

            if response:
                r = json.loads(response)
                tree = html.fromstring(r['html_content'])
                csto = tree.xpath('//div[@data-behavior="comment"]')
                for st in csto:
                    stocktalks.append(self.check_stocktals(st))

            page += 1

            if page == int(r['page_count']):
                break
            time.sleep(1)

        i = len(stocktalks)
        return stocktalks


def load_author_id(author_queue):
    # author_queue.put(3022051)

    with open('sa_authors.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        count = 0
        for row in reader:
            res = re.findall('/(\d+)$', row['url'])
            if res:
                author_id = int(res[0])
                author_queue.put(author_id)
                count += 1


def process_author(n, user_queue, pg_dsn, proxy):
    credential = settings.sa_logins[random.randrange(0, len(settings.sa_logins))]
    login = credential['login']
    password = credential['password']
    error_count = 0
    while True:
        try:
            sa = SeekingAlpha(proxy=proxy, login=login, password=password, logname='sa', n=n)
        except LoadingError:
            error_count += 1
            print('{} Login error {}'.format(n, error_count))
            if error_count > 5:
                return
            time.sleep(60)
            continue
        break

    sa.user_queue = user_queue

    # db_engine = create_engine(pg_dsn, pool_size=1)
    # add_engine_pidguard(db_engine)
    # DstSession = sessionmaker(bind=db_engine, autoflush=False)
    # session = DstSession()

    print('Scraper {} with Proxy {} started'.format(n, proxy))
    while True:
        r = user_queue.get()
        if r:
            userid = r
            try:
                scrape_author(sa, userid, session)
            except Exception as e:
                print(traceback.format_exc())
                user_queue.put(userid)
        else:
            break


def session_commit(session, name):
    try:
        session.commit()
    except sqlalchemy.exc.IntegrityError as err:
        if re.search('duplicate key value violates unique constraint', err.args[0]):
            print('ROLLBACK', name)
            session.rollback()


def fill_comment(sa, session, comm, article_db):
    print('{} Get comment {}'.format(sa.n, comm['id']))
    comment_db = session.query(Comment).filter_by(comment_id=comm['id']).first()
    if not comment_db:
        comment_db = Comment(
            comment_id=comm['id'],
            publish_date=comm['created_on'][0:10],
            publish_time=comm['created_on'][11:19],
            comment_likes=int(comm['likes']),
            comments_text=comm['content'],
            comment_url='https://seekingalpha.com' + comm['uri'],
            comment_timezone=comm['created_on'][23:],
            comment_type=None,
            comment_parent_id=comm['parent_id'])
        session.add(comment_db)
        article_db.comments.append(comment_db)
        author_db, is_new = fill_author(sa, session, comm['user_id'])
        if is_new:
            print('Add user', author_db.author_id)
            sa.user_queue.put(author_db.author_id)
        author_db.comments.append(comment_db)
        session_commit(session, 'comment')
    for comm2 in comm['children']:
        fill_comment(sa, session, comm['children'][comm2], article_db)


def fill_author(sa, session, userid):
    print('{} Get author {}'.format(sa.n, userid))
    author_db = session.query(Author).filter_by(author_id=userid).first()
    is_new = False
    if not author_db:
        author = sa.get_author_by_id(userid)
        author_db = Author(
            author_id=userid,
            author_name=author['author_name'],
            author_url=author['author_url'],
            member_since=author['member_since'],
            contributor_since=author['contributor_since'],
            strategy=author['strategy'],
            intro=author['intro'],
            articles_written=author['articles_written'],
            comments_posted=author['comments_posted'],
            stocktalks_count=author['stocktalks'],
            instablogs_count=author['instablogs_count'],
            followers_count=author['followers_count'],
            following_count=author['following_count'],
            company_name=author['company_name'],
            author_website=author['author_website'],
            twitter_id=author['twitter_id'] if author['twitter_id'] else None,
            author_slug=author['author_slug'])
        session.add(author_db)
        session_commit(session, 'author')
        is_new = True
    return author_db, is_new


def fill_instablog(sa, session, instablog, author_db):
    print('{} Get instablog {}'.format(sa.n, instablog['blog_id']))
    instablog_db = session.query(Blogposts).filter_by(blog_id=instablog['blog_id']).first()
    if not instablog_db:
        instablog_db = Blogposts(
            blog_id=instablog['blog_id'],
            blog_name=instablog['blog_name'],
            blogpost_id=instablog['blogpost_id'],
            blogpost_title=instablog['blogpost_title'],
            publish_date=instablog['publish_date'],
            publish_time=instablog['publish_time'],
            blog_timezone=instablog['blog_timezone'],
            blogpost_includes=instablog['blogpost_includes'],
            blogpost_comments=instablog['comments'],
            blogpost_replies=instablog['blogpost_replies'],
            blogpost_url=instablog['blogpost_url'],
            blogpost_links=instablog['blogpost_links'])
        session.add(instablog_db)
        author_db.blogposts.append(instablog_db)
        session_commit(session, 'blogpost')


def fill_article(sa, session, article_url, author_db):
    print('{} Get article {}'.format(sa.n, article_url))
    article_id = int(re.findall('https://seekingalpha.com/article/(\d+)-.+?', article_url)[0])
    article_db = session.query(Article).filter_by(article_id=article_id).first()
    if not article_db:
        article = sa.get_article_by_url(article_url)
        article_db = Article(
            article_id=article_id,
            article_url=article['article_url'],
            article_title=article['article_title'],
            published_date=article['published_date'],
            published_time=article['published_time'],
            disclosure=article['disclosure'],
            article_summary=article['article_summary'],
            article_text=article['article_text'],
            comments_received=article['comments_received'],
            article_likes=article['article_likes'],
            article_timezone=article['article_timezone'])

        author_db.articles.append(article_db)
        tickers = set(article['about_ticker'])
        for ticker in tickers:
            ticker_db = Article_tickers(ticker=ticker, type=None)
            session.add(ticker_db)
            article_db.article_tickers.append(ticker_db)

        for media_url in article['media_urls']:
            media_url_db = Article_urls(url=media_url)
            session.add(media_url_db)
            article_db.article_urls.append(media_url_db)

        for comm in article['comments']:
            comm = article['comments'][comm]
            fill_comment(sa, session, comm, article_db)

        session.add(article_db)
        session_commit(session, 'article')
    return article_db


def fill_stocktals(sa, session, st):
    print('{} Get stocktalk {}'.format(sa.n, st['stocktalk_id']))
    st_db = session.query(Stocktalks).filter_by(stocktalk_id=st['stocktalk_id']).first()
    if not st_db:
        st_db = Stocktalks(
            stocktalk_id=st['stocktalk_id'],
            publish_date=st['publish_date'],
            publish_time=st['publish_time'],
            stocktalk_timezone=st['stocktalk_timezone'],
            stocktalk_likes=st['stocktalk_likes'],
            stocktalk_text=st['stocktalk_text'],
            stocktalk_url=st['stocktalk_url'])
        session.add(st_db)
        author_db, is_new = fill_author(sa, session, st['author_id'])
        if is_new:
            # print('Add user', author_db.author_id)
            sa.user_queue.put(author_db.author_id)
        author_db.stocktalks.append(st_db)
        session_commit(session, 'stocktalk')
    return st_db


def scrape_author(sa, userid, session):
    # sa.get_stocktalks_by_userid('sa-eli-hoffmann',6383)
    articles = []
    # article = sa.get_article_by_url('https://seekingalpha.com/article/4153562-will-weak-guidance-sink-target')
    author_db, is_new = fill_author(sa, session, userid)
    if author_db.articles_written:
        article_urls = sa.load_article_urls_by_userid(author_db.author_slug, userid)
        for article_url in article_urls:
            article_db = fill_article(sa, session, article_url, author_db)

    # if author['comments_posted']:
    #     comments=sa.get_comments_by_userid(userid)
    if author_db.instablogs_count:
        instablogs = sa.get_instablog_by_userid(userid)
        for instablog in instablogs:
            instablog_db = fill_instablog(sa, session, instablog, author_db)
    if author_db.stocktalks_count:
        stocktalks = sa.get_stocktalks_by_userid(author_db.author_slug, userid)
        for st in stocktalks:
            st_db = fill_stocktals(sa, session, st)

    # sa.get_article_by_url('https://seekingalpha.com/article/4158069-exxon-mobil-think')
    # sa.get_article_by_url('https://seekingalpha.com/article/4157824-biotechs-approaching-new-waters')
    # sa.get_article_by_url('https://seekingalpha.com/instablog/6383-sa-eli-hoffmann/4951571-will-argos-defy-feuerstein-ratain-rule')
    # sa.get_instablog_by_userid(6383)
    # sa.get_stocktalks_by_userid(6383)
    # sa.get_author_by_id(427396)
    # sa.get_article_by_url('https://seekingalpha.com/article/4157824-biotechs-approaching-new-waters')


if __name__ == '__main__':
    # DstSession = sessionmaker(bind=db_engine, autoflush=False)
    dstssn = Session()

    author_queue = multiprocessing.Queue()

    pp = []
    p = Process(target=load_author_id, args=(author_queue, ))
    p.start()
    pp.append(p)

    for idx, s in enumerate(settings.proxy_list):
        p = Process(target=process_author, args=(idx + 1, author_queue, pg_dsn, s))
        p.start()
        pp.append(p)

    for p in pp:
        p.join()
