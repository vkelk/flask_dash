import concurrent.futures as cf
from datetime import datetime

from fintweet.models import Session, ScopedSession, Tweet, User, TweetMentions

session = Session()


def get_empty_permalinks():
    tweets = session.query(Tweet.tweet_id, User.twitter_handle) \
        .join(User) \
        .filter(Tweet.permalink == None).yield_per(10)
    return tweets


def main_worker(t):
    global global_count
    global global_time
    session = ScopedSession()
    tweet = session.query(Tweet).filter(Tweet.tweet_id == t.tweet_id).first()
    tweet.permalink = 'https://twitter.com/{}/status/{}'.format(t.twitter_handle, t.tweet_id)
    global_count += 1
    print(tweet.permalink)
    try:
        session.add(tweet)
        session.commit()
        if global_count % 1000 == 0:
            print('Average updates per sec:',
                  global_count / ((datetime.now() - global_time).total_seconds()))
            print(total_empty - global_count, 'left to populate')
    except Exception as e:
        print(type(e), str(e))


if __name__ == '__main__':
    total_empty = 4996710
    time_start = datetime.now()
    global_count = 0
    global_time = datetime.now()
    # for t in get_empty_permalinks():
    #     main_worker(t)
    # total_time = datetime.now() - time_start
    # print(total_time)
    # exit()

    with cf.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(main_worker, get_empty_permalinks())

    total_time = datetime.now() - time_start
    print('ALL DONE in', total_time)
