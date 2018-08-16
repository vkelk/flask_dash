DROP MATERIALIZED VIEW "fintweet"."mv_cashtags";

CREATE MATERIALIZED VIEW "fintweet"."mv_cashtags"
AS
SELECT row_number() OVER (ORDER BY t.tweet_id) AS id,
    t.tweet_id,
    t.user_id,
    c.cashtags,
		t.time at time zone 'UTC',
    (t.date + t."time") at time zone 'UTC' at time zone 'America/New_York' AS datetime
   FROM (fintweet.tweet_cashtags c
     JOIN fintweet.tweet t ON ((t.tweet_id = c.tweet_id)))
  WHERE (t.date between '2012-01-01'::date and '2017-01-01'::date)
  ORDER BY t.tweet_id;;

ALTER MATERIALIZED VIEW "fintweet"."mv_cashtags" OWNER TO "postgres";

