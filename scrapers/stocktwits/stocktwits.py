import os.path
import csv
import datetime
import json
from pprint import pprint
from multiprocessing.dummy import Process, Lock, Queue
from multiprocessing.dummy import Pool as ThreadPool
import re
import requests
import time
from openpyxl import load_workbook
from datetime import datetime
import settings
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy.event as event
import warnings

from sqlalchemy.orm import relationship
import sqlalchemy.exc
import os

os.environ['MKL_NUM_THREADS'] = '1'
os.environ['OMP_NUM_THREADS'] = '1'
os.environ['MKL_DYNAMIC'] = 'FALSE'

pg_config = {
    'username': settings.PG_USER,
    'password': settings.PG_PASSWORD,
    'database': settings.PG_DBNAME,
    'host': settings.DB_HOST
}
pg_dsn = "postgresql+psycopg2://{username}:{password}@{host}/{database}".format(**pg_config)


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
murl = 'mysql+pymysql://{}:{}@{}:{}/?charset=utf8mb4&use_unicode=1'
db_engine = create_engine(pg_dsn, pool_size=1)
add_engine_pidguard(db_engine)
pg_meta = MetaData(bind=db_engine, schema="stocktwits")


class User(Base):
    __table__ = Table('user', pg_meta, autoload=True)
    ideas = relationship('Ideas')
    counts = relationship('User_Count')
    strategy = relationship('UserStrategy')


class User_Count(Base):
    __table__ = Table('user_count', pg_meta, autoload=True)


class UserStrategy(Base):
    __table__ = Table('user_strategy', pg_meta, autoload=True)


class Ideas(Base):
    __table__ = Table('ideas', pg_meta, autoload=True)
    counts = relationship('Ideas_Count')
    cash_s = relationship('IdeasCashtags')
    hash_s = relationship('IdeasHashtags')
    url_s = relationship('IdeasUrls')


class Ideas_Count(Base):
    __table__ = Table('ideas_count', pg_meta, autoload=True)


class IdeasCashtags(Base):
    __table__ = Table('idea_cashtags', pg_meta, autoload=True)


class IdeasHashtags(Base):
    __table__ = Table('idea_hashtags', pg_meta, autoload=True)


class IdeasUrls(Base):
    __table__ = Table('ideas_url', pg_meta, autoload=True)


class Reply(Base):
    __table__ = Table('reply', pg_meta, autoload=True)


class LoadingError(Exception):
    pass


emoticons_str = r"""
    (?:
        [:=;] # Eyes
        [oO\-]? # Nose (optional)
        [D\)\]\(\]/\\OpP] # Mouth
    )"""

regex_str = [
    emoticons_str,
    r'<[^>]+>',    # HTML tags
    r'(?:@[\w_]+)',    # @-mentions
    r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)",    # hash-tags
    r"(?:\$+[a-zA-Z]+[\w\'_\-]*[\w_]+)",    # cash-tags
    r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+',    # URLs
    r'(?:(?:\d+,?)+(?:\.?\d+)?)',    # numbers
    r"(?:[a-z][a-z'\-_]+[a-z])",    # words with - and '
    r'(?:[\w_]+)',    # other words
    r'(?:\S)'    # anything else
]

tokens_re = re.compile(r'(' + '|'.join(regex_str) + ')', re.VERBOSE | re.IGNORECASE)
emoticon_re = re.compile(r'^' + emoticons_str + '$', re.VERBOSE | re.IGNORECASE)


def tokenize(s):
    return tokens_re.findall(s)


def preprocess(s, lowercase=False):
    tokens = tokenize(s)
    if lowercase:
        tokens = [token if emoticon_re.search(token) else token.lower() for token in tokens]
    return tokens


class Page(object):
    def __init__(self, proxy=None):
        self.pr = {'http': proxy, 'https': proxy}
        self.timeout = 30
        self.ses = requests.Session()
        self.ses.headers = {
            'user-agent':
            'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
        # 'Connection': 'keep-alive',
        # 'Cache-Control': 'max-age=0',
            'Accept-Encoding':
            'gzip, deflate, br',
            'Origin':
            'https://stocktwits.com',
        # 'accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept':
            '*/*',
            'Accept-Language':
            'en-US;q=0.6,en;q=0.4',
            'x-compress':
            None,
        # 'Upgrade-Insecure-Requests': '1',
        # 'x-requested-with': 'XMLHttpRequest',
        # 'x-twitter-active-user': 'yes',
        # 'host': 'stocktwits.com'
        }

    # def __del__(self):
    #     self.ses.close()
    #
    # def close(self):
    #     self.ses.close()

    def load(self, n, typ, url, params=None, headers=None, important=True):
        error_count = 0
        while True:
            try:
                if typ == 'get':
                    resp = self.ses.get(url, params=params, headers=headers, timeout=self.timeout)
                elif typ == 'put':
                    resp = self.ses.put(
                        url, data=json.dumps(params), headers=headers, timeout=self.timeout)
                elif typ == 'post':
                    resp = self.ses.post(
                        url, data=json.dumps(params), headers=headers, timeout=self.timeout)
                elif typ == 'delete':
                    resp = self.ses.delete(
                        url, data=json.dumps(params), headers=headers, timeout=self.timeout)

            except requests.exceptions.RequestException as e:
                error_count += 1
                # print('Loading error', error_count, pr, e)
                if error_count < 3:
                    print(n, 'Loading error', error_count, url, self.pr, e)
                    time.sleep(60)
                    continue
                print(n, 'Error limit exceeded Loading error', url, self.pr, e)
                raise LoadingError

            if resp.status_code == requests.codes.ok:
                return resp.text
            elif resp.status_code == 429:
                print(n, 'Rate limit. Sleep 3 min')
                time.sleep(3 * 60)
                continue
            elif resp.status_code == 502 or resp.status_code == 504:
                print(
                    n,
                    'wait 30 sec Error',
                    resp.status_code,
                    error_count,
                    url,
                    params,
                )
                error_count += 1
                if error_count < 5:
                    time.sleep(30)
                else:
                    if important:
                        raise LoadingError
                    else:
                        return None
            elif resp.status_code == 503:
                # print('503 Error waiting 2 min', screen_name, proxy, url, resp.text, resp.status_code)
                error_count += 1
                if error_count > 5:
                    print(n, 'AWAM: Requestes 503 error', url, self.pr)
                    raise LoadingError
                print(n, ' Requestes 503 error', error_count, url, self.pr)
                time.sleep(120)
                continue
            else:
                error_count += 1
                print(n, resp.text)
                print(n, 'Error', error_count, url, params, resp.status_code)
                if not important:
                    return None

                if error_count > 5:
                    print(n, 'Error limit exceeded Requestes error ', url, self.pr,
                          resp.status_code)
                    raise LoadingError
                print(n, 'Error waiting 1 min', url, resp.status_code)
                time.sleep(60)
                continue


def get_symbols(s):
    s = s.upper()
    res = re.findall('(\$[A-Z]{1,6}([._][A-Z]{1,2})?)', s, re.M)
    if res:
        r = list(map(lambda x: x[0], res))
        r = list(map(lambda x: x, r))
        r = list(set(r))
        return r
    else:
        return []


def get_new_search(n, page, proxy, query):
    # page = Page(proxy)
    params = {}

    query = query.strip('$').upper()

    url = 'https://stocktwits.com/symbol/' + query
    r = page.load(n, 'get', url)
    params = {}
    params['filter'] = 'all'
    params['max'] = None
    u = 'https://api.stocktwits.com/api/2/streams/symbol/' + query + '.json'
    while True:
        r = page.load(n, 'get', u, params=params, headers={'referer': url})
        j = json.loads(r)
        if j['response']['status'] == 200:
            for message in j['messages']:
                # t = get_details(page, message, query)
                yield message
            if j['cursor']['more']:
                params['max'] = j['cursor']['max']
                continue


def login(n, page):
    url = 'https://stocktwits.com/api/login'
    h = {'content-type': 'application/json'}
    params = {}
    params['user_session'] = {}
    params['user_session']['login'] = settings.stocktwits_login
    params['user_session']['password'] = settings.stocktwits_password

    r = page.load(n, 'post', url, params=params, headers=h)
    try:
        j = json.loads(r)
        page.ses.headers['authorization'] = 'OAuth ' + j['token']
    except Exception as e:
        print(e)
        print(n, 'Unable login')
        exit()
    print(n, 'Login success')


def get_tweets(n, dateto, permno, proxy, query, lock, session):
    query = query.strip('$').upper()
    count = 0
    page = Page(proxy)
    login(n, page)
    for t in get_new_search(n, page, proxy, query):
        count_repl = 0
        t1 = datetime.strptime(t['created_at'], '%Y-%m-%dT%H:%M:%SZ')
        # print(dateto,t1)
        if dateto > t1:
            break

        idea = session.query(Ideas).filter_by(ideas_id=t['id']).first()
        if idea:
            continue

        user = session.query(User).filter_by(user_id=t['user']['id']).first()
        if not user:
            if settings.ISUSERPROFILE:
                url = 'https://stocktwits.com/api/user_info_stream/' + t['user']['username'] + '?limit=15'
                r = page.load(
                    n,
                    'get',
                    url,
                    headers={'referer': 'https://stocktwits.com/' + t['user']['username']},
                    important=False)
                if r:
                    j = json.loads(r)
                    t['user'] = j['user']
            if settings.ISWATCHLIST:
                url = 'https://api.stocktwits.com/api/2/watchlists/user/' + str(
                    t['user']['id']) + '.json'
                r = page.load(
                    n,
                    'get',
                    url,
                    headers={'referer': 'https://stocktwits.com/' + t['user']['username']},
                    important=False)
                if r:
                    j = json.loads(r)
                    watch_list = ' '.join([f['symbol'] for f in j['watchlist']['symbols']])[:500]
            else:
                watch_list = None
            user = session.query(User).filter_by(user_id=t['user']['id']).first()
            if not user:
                user = User(
                    user_id=t['user']['id'],
                    user_name=t['user']['name'],
                    user_handle=t['user']['username'],
                    date_joined=t['user']['join_date'],
                    website=t['user']['website_url'],
                    source=None,
                # user_strategy=json.dumps(t['user']['trading_strategy'])[:200],
                    user_topmentioned=' '.join(
                        [f['symbol'] for f in t['user']['most_mentioned'][0]])[:255]
                    if t['user'].get('most_mentioned', False) else None,
                    verified='YES' if t['user']['official'] else 'NO',
                    location=t['user']['location'][:255]
                    if t['user'].get('location', False) else None)

                usr_st_dict = t['user']['trading_strategy']
                assets_frequently_traded = json.dumps(usr_st_dict['assets_frequently_traded'])
                user_strategy = UserStrategy(
                    user_id=t['user']['id'],
                    assets_frequently_traded=assets_frequently_traded,
                    approach=usr_st_dict['approach'],
                    holding_period=usr_st_dict['holding_period'],
                    experience=usr_st_dict['experience'])

                user_count = User_Count(
                    followers=t['user']['followers'],
                    following=t['user']['following'],
                    watchlist_count=t['user']['watchlist_stocks_count'],
                    watchlist_stocks=watch_list,
                    ideas=t['user']['ideas'])
                user.counts.append(user_count)
                user.strategy.append(user_strategy)
                session.add(user_count)
                session.add(user_strategy)
                session.add(user)
                try:
                    session.commit()
                except sqlalchemy.exc.IntegrityError as err:
                    if re.match("(.*)Duplicate entry(.*)for key 'PRIMARY'(.*)", err.args[0]):
                        print(n, 'ROLLBACK USER')
                        session.rollback()
                        # print('DUBLICATE USER', err)

        idea = Ideas(
            ideas_id=t['id'],
            permno=permno,
            date=t1.date(),
            time=t1.time(),
            replied='YES'
            if t.get('conversation', False) and int(t['conversation']['replies']) > 0 else 'NO',
            text=t['body'],
            sentiment=t['entities']['sentiment']['basic'] if t['entities']['sentiment'] else None,
        # cashtags_other=' '.join([f['symbol'] for f in t['symbols'] if f['symbol'] != query.upper()])[:255]
        )
        if t.get('conversation', False) and settings.ISREPLY:
            count_repl = 0
            if int(t['conversation']['replies']) > 20:
                rrr = 1
            max = None
            url = 'https://api.stocktwits.com/api/2/messages/' + str(
                t['id']) + '/conversation.json?max='
            flag_conv = False
            while True:

                r = page.load(n, 'get', url, important=False)
                if not r:
                    print(n, 'Error loading conversation')
                    break    # , headers={'referer': 'https://stocktwits.com/' + t['user']['username']})
                j = json.loads(r)
                if flag_conv:
                    mess = j
                else:
                    mess = j['children']
                for children in mess['messages']:
                    count_repl += 1
                    reply = session.query(Reply).filter_by(reply_id=children['id']).first()
                    if not reply:
                        t1 = datetime.strptime(children['created_at'], '%Y-%m-%dT%H:%M:%SZ')
                        reply = Reply(
                            reply_id=children['id'],
                            date=t1.date(),
                            time=t1.time(),
                            reply_userid=children['user']['id'],
                            text=children['body'])
                        session.add(reply)
                        try:
                            session.commit()
                        except sqlalchemy.exc.IntegrityError as err:
                            if re.match("(.*)Duplicate entry(.*)for key 'PRIMARY'(.*)",
                                        err.args[0]):
                                print(n, 'ROLLBACK REPLY')
                                session.rollback()

                    idea.replies.append(reply)
                # break
                if mess['cursor']['more']:
                    url = 'https://api.stocktwits.com/api/2/messages/' + str(
                        t['id']) + '/children.json?since=' + str(mess['cursor']['since'])
                    flag_conv = True
                    continue
                break

        idea_count = Ideas_Count(
            replies=t['conversation']['replies'] if t.get('conversation', False) else None,
            likes=t['likes']['total'] if t.get('likes', False) else None)
        idea.counts.append(idea_count)

        tokens = preprocess(idea.text)
        cashtags = [term for term in tokens if term.startswith('$') and len(term) > 1]
        if len(cashtags) > 0:
            for cashtag in cashtags:
                ctag = IdeasCashtags(ideas_id=idea.ideas_id, cashtag=cashtag)
                idea.cash_s.append(ctag)
                session.add(ctag)
        hashtags = [term for term in tokens if term.startswith('#') and len(term) > 1]
        if len(hashtags) > 0:
            for hashtag in hashtags:
                htag = IdeasHashtags(ideas_id=idea.ideas_id, hashtag=hashtag)
                idea.hash_s.append(htag)
                session.add(htag)
        urls = [term for term in tokens if term.startswith('http') and len(term) > 4]
        if len(urls) > 0:
            for u in urls:
                utag = IdeasHashtags(ideas_id=idea.ideas_id, hashtag=u)
                idea.url_s.append(utag)
                session.add(utag)

        # if t.get('links', False):
        #     link = 'https://stocktwits.com/' + t['user']['username'] + '/message/' + str(t['id'])
        #     idea_url = Ideas_Url(link=link[:150],
        #                          url=' '.join([f['url'] for f in t['links']])[:150] if t.get('links', False) else None)
        #     idea.urls.append(idea_url)
        #     session.add(idea_url)

        user.ideas.append(idea)
        session.add(idea_count)
        session.add(idea)
        try:
            session.commit()
        except sqlalchemy.exc.IntegrityError as err:

            if re.match("(.*)Duplicate entry(.*)for key 'PRIMARY'(.*)", err.args[0]):
                print(n, 'ROLLBACK IDEAS')
                session.rollback()
                # print('DUBLICATE USER', err)

        count += 1
        print('{}   {:6} {:8}  {}  {}'.format(n, query, count, t['created_at'], count_repl
                                              if count_repl > 0 else ''))

        # if count>99: break

    if count:
        fname = 'report.csv'
        fdnames = [
            'time',
            'cashtag',
            'permno',
            'number',
        ]
        data = {}
        data['time'] = time.strftime('%Y-%m-%d %H:%M:%S')
        data['cashtag'] = query

        data['permno'] = permno
        data['number'] = count

        if os.path.isfile(fname):

            with open(fname, 'a', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(
                    csvfile,
                    lineterminator='\n',
                    fieldnames=fdnames,
                    dialect='excel',
                    quotechar='"',
                    quoting=csv.QUOTE_ALL)
                writer.writerow(data)

        else:

            with open(fname, 'w') as f:

                writer = csv.DictWriter(
                    f,
                    lineterminator='\n',
                    fieldnames=fdnames,
                    dialect='excel',
                    quotechar='"',
                    quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerow(data)

    else:
        pass

    return count


def scrape(n, user_queue, proxy, lock, pg_dsn):
    db_engine = create_engine(pg_dsn, pool_size=1)
    add_engine_pidguard(db_engine)
    # db_engine.execute('USE `stocktwits`')

    # db_engine.execute('SET NAMES utf8mb4;')
    # db_engine.execute('SET CHARACTER SET utf8mb4;')
    # db_engine.execute('SET character_set_connection=utf8mb4;')
    pg_meta = MetaData(bind=db_engine, schema="stocktwits")
    Session = sessionmaker(bind=db_engine)
    session = Session()

    while not user_queue.empty():
        dateto, permno, query, i = user_queue.get()
        n = i
        print(n, 'START', i, proxy, query)
        try:
            res = get_tweets(n, dateto, permno, proxy, query, lock, session=session)
        except LoadingError:
            print(n, 'LoadingError except')
            return
        if not res:
            print(n, '     SCRAP_USER Error in', query, i)
            # with open('error_list.txt', 'a') as f:
            #     f.write(query[0] + '\n')
        else:
            print(n, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), ' ENDED', i, proxy, query, res)

        time.sleep(1)


if __name__ == '__main__':
    # multiprocessing.set_start_method('forkserver')

    Session = sessionmaker(bind=db_engine)
    session = Session()

    user_queue = Queue()
    lock = Lock()

    fname = 'symbol_list.xlsx'
    wb = load_workbook(fname)
    ws = wb.active
    i = 2
    while True:
        if not ws.cell(row=i, column=1).value:
            break
        permno = str(ws.cell(row=i, column=1).value).lower().strip(' ')
        query = str(ws.cell(row=i, column=2).value).lower().strip(' ')
        dd = ws.cell(row=i, column=3).value
        dateto = dd if isinstance(dd, datetime) else datetime.strptime(dd, '%d/%m/%Y')

        # print(query)
        user_queue.put((dateto, permno, query, i - 1))
        i += 1

    np = len(settings.proxy_list) if len(settings.proxy_list) > i - 2 else i - 2
    pp = []
    proxy_list = []
    n = 0
    for i, proxy in enumerate(settings.proxy_list):
        n += 1
        proxy_list.append({'proxy': proxy, 'num': n})
    pool = ThreadPool(len(settings.proxy_list))
    while True:
        pool.map(lambda x: (scrape(x['num'], user_queue, x['proxy'], lock, pg_dsn)), proxy_list)
        pool.close()
        pool.join()
        if not user_queue.empty():
            continue
    # for n,i in enumerate(range(np)):
    #
    #     p = Process(target=scrape, args=(n+1, user_queue, settings.proxy_list[i], lock, murl))
    #     p.start()
    #     pp.append(p)
    #     #
    # for p in pp:
    #     p.join()
