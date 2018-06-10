import logging
import requests
import time
import re
from pyquery import PyQuery
import sys
import json

from datetime import datetime


class LoadingError(Exception):
    pass


class Twit():
    pass


class Page(object):
    def __init__(self, proxy=None):
        self.logger = logging.getLogger(__name__)
        self.pr = {'http': proxy, 'https': proxy}
        self.timeout = 30
        self.ses = requests.Session()
        self.ses.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36',
            'Connection': 'keep-alive',
            'Cache-Control': 'max-age=0',
            'Accept-Encoding': 'gzip, deflate, sdch, br',
            # 'accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US;q=0.6,en;q=0.4',
            'x-compress': 'null',
            'Upgrade-Insecure-Requests': '1',
            # 'x-requested-with': 'XMLHttpRequest',
            # 'x-twitter-active-user': 'yes',
            'host': 'twitter.com'
        }

    def __del__(self):
        self.ses.close()

    def close(self):
        self.ses.close()

    def load(self, url, params=None, headers=None, important=False):
        error_count = 0
        while True:
            try:
                resp = self.ses.get(url, proxies=self.pr, params=params, headers=headers, timeout=self.timeout)
            except requests.exceptions.RequestException as e:
                error_count += 1
                # print('Loading error', error_count, pr, e)
                if error_count < 3:
                    # self.logger.warning('Loading error %s %s %s', error_count, url, self.pr, e)
                    time.sleep(60)
                    continue
                self.logger.error('HTTP limit exceeded Loading error %s %s %s', url, self.pr, e)
                if important:
                    raise LoadingError
                else:
                    return None

            if resp.status_code == requests.codes.ok:
                return resp.text
            elif resp.status_code == 404:
                self.logger.warning('HTTP %s %s %s', url, resp.status_code, resp.text)
                err_response = self.handle_err_response(resp.text)
                if err_response:
                    return err_response
                self.logger.error('%s %s %s', resp.staus_code, resp.text, url)
                if important:
                    raise LoadingError
                else:
                    return None
            elif resp.status_code == 403:
                self.logger.warning('HTTP %s %s %s', url, resp.status_code, resp.text)
                err_response = self.handle_err_response(resp.text)
                if err_response:
                    return err_response
                self.logger.error('HTTP %s %s %s', resp.staus_code, resp.text, url)
                if resp.text == '{"message":"Sorry, that user is suspended."}':
                    return resp.text
                raise
                return None
            elif resp.status_code == 429:
                self.logger.warn('HTTP %s Rate limit. Sleep 3 min %s', resp.status_code, url)
                time.sleep(3 * 60)
                continue
            elif resp.status_code == 503:
                self.logger.warn('HTTP %s Error waiting 2 min %s', resp.status_code, url)
                error_count += 1
                if error_count > 5:
                    self.logger.error('HTTP %s %s', resp.status_code, url)
                    if important:
                        raise LoadingError
                    else:
                        return None
                time.sleep(120)
                continue
            else:
                error_count += 1
                self.logger.warn('HTTP %s %s Count: %s', resp.status_code, url, error_count)
                if error_count > 5:
                    self.logger.error(
                        'HTTP %s %s %s Error limit exceeded Requests error Count: %s',
                        resp.status_code, url, self.pr, error_count)
                    if important:
                        raise LoadingError
                    else:
                        return None
                self.logger.warn('HTTP %s %s waiting 1 min', resp.status_code, url)
                time.sleep(60)
                continue

    def handle_err_response(self, err_msg):
        try:
            msg = json.loads(err_msg)
            if 'message' in msg:
                return err_msg
        except Exception as e:
            self.logger.exception('message')
        self.logger.error('Could not handle %s', err_msg)
        return None


class TweetScraper(object):
    def __init__(self, proxy=None, IS_PROFILE_SEARCH=None, logname=None):
        self.logger = logging.getLogger(__name__)
        self.IS_PROFILE_SEARCH = IS_PROFILE_SEARCH
        self.page = Page(proxy)
        self.ISUSERPROFILE = True

    def twitter_login(self, login, password):
        resp = self.page.load('https://twitter.com/')

        res = re.findall('<input type="hidden" value="(.+?)" name="authenticity_token">', resp)
        token = res[0]

        params = {'session[username_or_email]': login,
                  'session[password]': password,
                  'remember_me': '1',
                  'return_to_ssl': 'true',
                  'scribe_log': '',
                  'redirect_after_login': '/',
                  'authenticity_token': token}

        url = 'https://twitter.com/sessions'
        while True:
            resp = self.page.ses.post(url, data=params, timeout=10)
            if resp.status_code == requests.codes.ok:
                if re.search('action="/logout" method="POST">', resp.text):
                    self.logger.info('Logged as %s', login)

                    res = re.findall('<input type="hidden" value="(.+?)" name="authenticity_token', resp.text)
                    token = res[0]
                    return token
                elif re.search('Your account appears to have exhibited automated behavior that violates', resp.text):
                    self.logger.warning('Your account appears to have exhibited automated behavior that violates')
                    self.logger.warning('Pass a Google reCAPTCHA challenge.Verify your phone number')
                    return False

                elif re.search('id="login-challenge-form"', resp.text):

                    authenticity_token = re.findall('name="authenticity_token" value="(.+?)"/>', resp.text, re.S)[0]
                    challenge_id = re.findall('name="challenge_id" value="(.+?)"/>', resp.text, re.S)[0]
                    user_id = re.findall('name="user_id" value="(.+?)"/>', resp.text, re.S)[0]
                    challenge_type = re.findall('name="challenge_type" value="(.+?)"/>', resp.text, re.S)[0]
                    platform = re.findall('name="platform" value="(.+?)"/>', resp.text, re.S)[0]
                    if challenge_type == 'TemporaryPassword':
                        if re.search(
                                'In order to protect your account from suspicious activity, we&#39;ve sent a confirmation code to',
                                resp.text):
                            s = '   Enter {} for {}:'.format('TemporaryPassword', login)
                            challenge_response = input(s)
                            print(challenge_type)
                            return None
                    else:
                        self.logger.error('Challenge type: %s', challenge_type)
                        exit()
                    params = {
                        'authenticity_token': authenticity_token,
                        'challenge_id': challenge_id,
                        'user_id': user_id,
                        'challenge_type': challenge_type,
                        'platform': platform,
                        'redirect_after_login': '/',
                        'remember_me': 'true',
                        'challenge_response': challenge_response}
                    url = 'https://twitter.com/account/login_challenge'
                    print(challenge_type)
                    continue
                elif re.search('You have initiated too many login verification requests', resp.text, re.S):
                    self.logger.error('You have initiated too many login verification requests')
                    raise LoadingError

                else:
                    self.logger.warning('Not logged as %s', login)
                    return False
                break
            else:
                self.logger.error('Login Error %s', resp.status_code)
                return False

    def get_new_search(self, query, login=None, password=None, nativeretweets=False):
        user_name = query[0]
        data_begin = query[2]
        data_end = query[3]
        data_current = data_begin
        refreshCursor = ''

        query_string = query[1]

        h = {'x-requested-with': 'XMLHttpRequest', 'x-twitter-active-user': 'yes',
             'accept': 'application/json, text/javascript, */*; q=0.01'}
        params = {}
        if self.IS_PROFILE_SEARCH:
            refreshCursor = '999992735314882560'

            token = self.twitter_login(login, password)
            if not token:
                self.logger.error('UNABLE TO LOGIN')
                return False

        empty_count = 0
        date_range_change_count = 0
        while True:
            if self.IS_PROFILE_SEARCH:
                url = 'https://twitter.com/i/profiles/show/' + query_string + '/timeline/with_replies'
                # url = 'https://twitter.com/i/profiles/show/' + query_string + '/timeline/with_tweets'
                params['include_available_features'] = '1'
                params['include_entities'] = '1'
                params['reset_error_state'] = 'false'

            else:
                url = 'https://twitter.com/i/search/timeline'

                params['include_available_features'] = '1'
                params['include_entities'] = '1'
                params['reset_error_state'] = 'false'
                params['vertical'] = 'default'
                params['src'] = 'ctag'
                params['f'] = 'tweets'
                params['l'] = 'en'
                params['q'] = '"' + query_string + '" since:' + data_begin + ' until:' + data_end

            params['max_position'] = refreshCursor
            resp = self.page.load(url, params=params, headers=h)
            try:
                r = json.loads(resp)
                # pprint(r.keys())
            except Exception as e:
                # print(resp.text)
                self.logger.error('JSON error %s %s %s', url, self.page.pr, resp.text)
                self.logger.exception('message')
                raise LoadingError

            if not r.get('inner', False):
                r['inner'] = r
            # pprint(r.keys())
            try:
                refreshCursor = r['inner']['min_position']
            except KeyError:
                self.logger.warning('KeyError %s %s %s', url, self.page.pr, resp.text)
                # print(resp.text)
                # print('Key error', url, self.page.pr)
                raise LoadingError
            del resp

            if not refreshCursor:
                break

            # '\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n \n':
            if not re.sub('[\n ]', '', r['inner']['items_html']):
                empty_count += 1
                if empty_count > 3:
                    if data_current > data_begin:
                        self.logger.warning('Reduce date range %s', query_string)
                        # print('Reduce date range')
                        date_range_change_count += 1
                        if date_range_change_count < 3:
                            data_end = data_current
                            empty_count = 0
                            continue
                    break
                else:
                    # print(r['inner']['new_latent_count'])
                    # self.logger.warning('Twitter server stopped. sleep 3 sec %s', query_string)
                    # print('Twitter server stopped. sleep 3 sec')
                    time.sleep(3)
                    continue
            empty_count = 0
            date_range_change_count = 0
            r = r['inner']['items_html']
            for tweet in self.cont(r, query_string):
                if tweet:
                    data_current = re.sub(' \d+:\d+:\d+', '', str(tweet.date))
                    yield tweet

        self.page.close()

    def cont(self, r, query_string):
        # r = re.sub('</span><span class="invisible">', '', r)
        try:
            tweets = PyQuery(r)('div.js-stream-tweet')
            # print(tweets.html())
        except Exception as e:
            self.logger.warning('no div.js-stream-tweet')
            self.logger.exception('message')
            return None
        for tweetHTML in tweets:
            # pprint(tweetHTML)

            tweet = Twit()
            # tweet.c = user_tweet_count
            tweetPQ = PyQuery(tweetHTML)
            # pprint(tweetPQ)
            tweet.time_zone = ''  # time_zone
            res = re.findall('twitter-cashtag pretty-link js-nav" dir="ltr"><s>\$</s><b>(?:<strong>)?(.+?)</',
                             str(tweetPQ), re.M)
            tweet.symbols = []
            if res:
                res = list(set(res))
                for rt in res:
                    if '$' + rt.upper() != query_string.upper():
                        tweet.symbols.append('$' + rt.upper())

            else:
                tweet.symbols = []

            # print(PyQuery(tweetHTML)('a.twitter-timeline-link'))
            tweet.urls = []

            flag = False
            for aa in PyQuery(tweetHTML)('a.twitter-timeline-link'):
                aaa = PyQuery(aa)

                if aaa.attr('data-expanded-url') and aaa.attr('data-expanded-url') != 'null' and aaa.attr(
                        'data-expanded-url') != 'None':
                    tweet.urls.append(aaa.attr('data-expanded-url'))
                    flag = True
                    # print(tweetPQ("p.js-tweet-text").text())
                else:
                    tweet.urls.append(aaa.attr('href'))
                    # print(aaa.attr('href'),'https://'+aaa.text())
                # aaa.remove()
            # print(tweetPQ("p.js-tweet-text").text())
            # if flag:
            #     raise LoadingError

            tweet.mentions_name = []
            tweet.mentions_id = []
            tweet.ment_s = []
            for aa in PyQuery(tweetHTML)('a.twitter-atreply'):
                aaa = PyQuery(aa)
                # mention={'screen_name':aaa.attr('href').replace('/',''),'id':aaa.attr('data-mentioned-user-id')}
                tweet.ment_s.append((aaa.attr('href').replace('/', ''), aaa.attr('data-mentioned-user-id')))
                tweet.mentions_name.append(aaa.attr('href').replace('/', ''))
                tweet.mentions_id.append(aaa.attr('data-mentioned-user-id'))

            usernameTweet = tweetPQ.attr("data-screen-name")

            t = tweetPQ("p.js-tweet-text").text() \
                .replace('# ', '#') \
                .replace('@ ', '@') \
                .replace('http:// ', 'http://') \
                .replace('http://www. ', 'http://www.') \
                .replace('https://www. ', 'https://www.') \
                .replace('https:// ', 'https://')
            # t = tweetPQ("p.js-tweet-text").text()
            # print(tweetPQ("p.js-tweet-text").html())
            # print('tweetPQ START', t, 'END')
            e = tweetPQ('img.Emoji')
            tweet.emoji = []
            for em in e:
                tweet.emoji.append(str(PyQuery(em).attr('aria-label').replace('Emoji: ', '')))

            txt = re.sub(r"\s+", " ", t)
            txt = re.sub(r'\$ (?P<s>[A-Za-z][A-Za-z0-9]{0,4}\b)', '$\g<s>', txt)
            txt = re.sub(r'\# (?P<s>\w*[a-zA-Z]+\w*)', '#\g<s>', txt)
            txt = re.sub(r'\@ (?P<s>[\w_]+)', '@\g<s>', txt)

            if not re.search('<strong class="fullname">Tweet withheld</strong>', str(tweetPQ), re.M):
                try:
                    retweets = int(tweetPQ("span.ProfileTweet-action--retweet span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                except AttributeError:
                    self.logger.warning('AttributeError in ProfileTweet-action--retweet %s', str(tweetPQ))
                    # print(str(tweetPQ))
                    # print('Attribute error in ProfileTweet-action--retweet')
                    retweets = 0

                try:
                    favorites = int(tweetPQ("span.ProfileTweet-action--favorite span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                except AttributeError:
                    self.logger.warning('AttributeError in ProfileTweet-action--retweet %s', str(tweetPQ))
                    # print(str(tweetPQ))
                    # print('Attribute error in ProfileTweet-action--retweet')
                    favorites = 0

                try:
                    replyes = int(tweetPQ("span.ProfileTweet-action--reply span.ProfileTweet-actionCount").attr(
                        "data-tweet-stat-count").replace(",", ""))
                except AttributeError:
                    self.logger.warning('AttributeError in ProfileTweet-action--retweet %s', str(tweetPQ))
                    # print(str(tweetPQ))
                    # print('Attribute error in ProfileTweet-action--retweet')
                    replyes = 0

            dateSec = int(tweetPQ("small.time span.js-short-timestamp").attr("data-time"))

            if tweetPQ.attr('data-protected') == 'true':
                tweet.is_protected = True
            else:
                tweet.is_protected = False
            id = tweetPQ.attr("data-tweet-id")
            permalink = tweetPQ.attr("data-permalink-path")
            tweet.user_id = tweetPQ.attr('data-user-id')
            tweet.id = id
            tweet.permalink = 'https://twitter.com' + permalink
            tweet.screen_name = usernameTweet
            tweet.user_name = tweetPQ.attr('data-name')
            # txt = re.sub('(?:https\://)|(?:http\://)', '', txt)
            # txt = re.sub('https\:\/\/', '', txt)
            # txt=re.sub('http\:\/\/','',txt)
            # print('text START', txt, 'END')
            tweet.text = txt
            tweet.unixtime = dateSec
            tweet.date = datetime.fromtimestamp(dateSec)
            tweet.retweets_count = retweets
            tweet.favorites_count = favorites
            tweet.replyes_count = replyes
            # tweet.mentions = re.compile('(@\\w*)').findall(tweet.text)
            tweet.mentions = re.compile('(?:@[\w_]+)').findall(tweet.text)
            # tweet.hashtags = re.compile('(#\\w*)').findall(tweet.text)
            tweet.hashtags = re.compile('(?:\#+[\w_]+[\w\'_\-]*[\w_]+)').findall(tweet.text)

            tweet.geo = {}

            if tweetPQ.attr('data-retweeter'):
                tweet.is_retweet = True
                tweet.retweet_user_id = tweetPQ.attr('data-user-id')
                tweet.retweet_id = tweetPQ.attr('data-retweet-id')
            else:

                tweet.is_retweet = False

            tweet.lang = tweetPQ("p.js-tweet-text").attr("lang")

            tweet.is_reply = tweetPQ.attr("data-is-reply-to")
            tweet.data_conversation_id = tweetPQ.attr("data-conversation-id")
            if tweet.is_reply:

                tweet.is_reply = True
                tweet.data_conversation_id = tweetPQ.attr("data-conversation-id")
                tweet.is_reply_href = tweetPQ('a.js-user-profile-link').attr('href')
                tweet.is_reply_screen_name = tweet.is_reply_href.replace('/', '')
                tweet.is_reply_id = tweetPQ('a.js-user-profile-link').attr('data-user-id')
                if tweet.is_reply_id == tweet.user_id:  # reply to self
                    tweet.is_reply_username = tweet.user_name
                else:
                    tt = re.findall('<span class="username(.+?)</span>', str(tweetPQ('a.js-user-profile-link')),
                                    re.S | re.M)

                    try:
                        tweet.is_reply_username = re.findall('<b>(.+?)</b>', tt[0])[0]
                    except IndexError:
                        self.logger.warning(
                            'IndexError %s %s %s %s',
                            tweetPQ('a.js-user-profile-link'),
                            tt, tweet.is_reply_id, tweet.permalink)
                        # print('ERROR', tweetPQ('a.js-user-profile-link'))
                        # print(tt, tweet.is_reply_id, tweet.permalink)
                        raise LoadingError
                        # print(tweet.is_reply_username)

                        # print(tweet.is_reply_href,tweet.is_reply_screen_name,tweet.is_reply_id,tweet.is_reply_username)
            else:
                tweet.is_reply = False
                # tweet.data_conversation_id = ''
                tweet.is_reply_href = ''
                tweet.is_reply_screen_name = ''
                tweet.is_reply_id = ''
                tweet.is_reply_username = ''

            tweet.likes = None
            tweet.user_tweet_count = None
            tweet.user_following_count = None
            tweet.user_followers_count = None
            tweet.user_created = None
            tweet.is_verified = None
            tweet.website = None
            tweet.user_location = None
            if self.ISUSERPROFILE:
                r = self.get_user_profile(usernameTweet,  tweet)
                if r:
                    tweet = r

            tweet.location_name = None
            tweet.location_id = None
            if True:  # ISLOCATION:
                url = 'https://twitter.com/' + tweet.screen_name + '/status/' + str(
                    tweet.data_conversation_id) + '?conversation_id=' + str(tweet.data_conversation_id)
                j = self.get_s(url, tweet.screen_name, important=False)

                if j and 'page' in j:
                    tweet_status = PyQuery(j['page'])('a.js-geo-pivot-link')
                    if tweet_status:
                        # print(tweet_status.text(), tweet_status.attr('data-place-id'))
                        tweet.location_name = tweet_status.text()
                        tweet.location_id = tweet_status.attr('data-place-id') if tweet_status.attr(
                            'data-place-id') else ''
                else:
                    self.logger.warning('Key "page" does not exists %s', url)
                    continue
            yield tweet
        return None

    def get_user_profile(self, usernameTweet,  tweet):
        url = 'https://twitter.com/' + usernameTweet
        try:
            j = self.get_s(url, '', important=False)  # query_string)
        except Exception as e:
            self.logger.error('%s %s', type(e), str(e))
            # fname = sys._getframe().f_code.co_name
            # print(fname, type(e), str(e))
            raise
        if j and ('init_data' in j or 'message' in j):
            if 'init_data' in j:
                if 'profile_user' in j['init_data']:
                    tweet.user_id = int(j['init_data']['profile_user']['id'])
                    tweet.likes = j['init_data']['profile_user']['favourites_count']
                    tweet.user_tweet_count = j['init_data']['profile_user']['statuses_count']
                    tweet.user_listed_count = j['init_data']['profile_user']['listed_count']
                    tweet.user_description = j['init_data']['profile_user']['description']
                    tweet.user_timezone = j['init_data']['profile_user']['time_zone']
                    tweet.utc_offset = j['init_data']['profile_user']['utc_offset']
                    tweet.user_following_count = j['init_data']['profile_user']['friends_count']
                    tweet.user_followers_count = j['init_data']['profile_user']['followers_count']
                    tweet.user_created = j['init_data']['profile_user']['created_at']
                    tweet.is_verified = j['init_data']['profile_user']['verified']
                    tweet.website = j['init_data']['profile_user']['url']
                    tweet.username = j['init_data']['profile_user']['name']
                    tweet.utc_offset = j['init_data']['profile_user']['utc_offset']
                    if j['init_data']['profile_user'].get('location', False):
                        tweet.user_location = j['init_data']['profile_user']['location']
                    else:
                        tweet.user_location = ''
                    return tweet
            if 'message' in j:
                if j['message'].strip() == 'This user does not exist.':
                    tweet.user_status = 'not_exists'
                    return tweet
                elif j['message'].strip() == 'Sorry, that user is suspended.':
                    tweet.user_status = 'suspended'
                    return tweet
                else:
                    self.logger.warning('Unhandeled error message: "%s"', j['message'])
        self.logger.warning('Unhandeled error response: "%s"', j)
        return None

    def get_s(self, url, screen_name, important=True):
        h = {'X-Requested-With': 'XMLHttpRequest', 'x-overlay-request': 'true', 'x-previous-page-name': 'profile'}
        resp = self.page.load(url, headers=h, important=important)
        j = resp
        if resp:
            try:
                j = json.loads(resp)
            except Exception as e:
                self.logger.error('Json decode error %s %s', url, resp)
                self.logger.exception('message')
                if important:
                    raise LoadingError
                else:
                    return None

        return j
