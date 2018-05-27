import concurrent.futures as cf

from fintweet.models import Session, ScopedSession, Tweet, User


def get_user(twitter_handle):
    subsession = ScopedSession()
    user = subsession.query(User).filter(User.twitter_handle == twitter_handle).first()
    if user:
        return user.user_id
    return None


def update_user(tweet):
    subsession = ScopedSession()
    twitter_handle = tweet.permalink.split('/')[3]
    user_id = get_user(twitter_handle)
    t = subsession.query(Tweet).filter(Tweet.tweet_id == tweet.tweet_id).first()
    if user_id:
        try:
            t.user_id = user_id
            subsession.add(t)
            subsession.commit()
            print('Updated', t.tweet_id, 'with user_id', user_id)
        except Exception as e:
            print(type(e), str(e))
    return t.tweet_id


if __name__ == '__main__':
    session = Session()
    # try:
    #     tweets = session.query(Tweet).filter(Tweet.user_id == None)
    #     for t in tweets.yield_per(100):
    #         update_user(t)
    # except Exception as e:
    #     print(type(e), str(e))
    #     raise
    # exit()

    tweets = session.query(Tweet).filter(Tweet.user_id == None).execution_options(stream_results=True).yield_per(100)
    with cf.ThreadPoolExecutor(max_workers=16) as executor:
        try:
            executor.map(update_user, tweets)
        except Exception as e:
            print(type(e), str(e))
            raise

    print("ALL DONE.")
