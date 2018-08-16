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

CREATE MATERIALIZED VIEW "stocktwits"."mv_cashtags"
AS
SELECT row_number() OVER (ORDER BY i.ideas_id) AS id,
    i.ideas_id,
    i.user_id,
    c.cashtag,
    timezone('America/New_York'::text, timezone('UTC'::text, i.datetime)) AS datetime
   FROM (stocktwits.idea_cashtags c
     JOIN stocktwits.ideas i ON ((i.ideas_id = c.ideas_id)))
  WHERE ((i.datetime >= '2012-01-01'::date) AND (i.datetime <= '2017-01-01'::date))
  ORDER BY i.ideas_id;