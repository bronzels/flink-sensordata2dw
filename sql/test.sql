SELECT COUNT(1) FROM sensordata_events_fm;
SELECT COUNT(1) FROM sensordata_users_fm;

SELECT time FROM sensordata_events_fm ORDER BY time DESC LIMIT 10;

-- firstusetime
-- logintime
-- registertime
-- register_date
-- regtime
-- track_time
-- getcode_time
-- broker_time
-- realname_time
-- event_time
-- response_time
-- recv_time

SELECT firstusetime FROM sensordata_events_fm WHERE firstusetime IS NOT NULL ORDER BY firstusetime DESC LIMIT 10;
SELECT logintime FROM sensordata_events_fm WHERE logintime IS NOT NULL ORDER BY logintime DESC LIMIT 10;
SELECT registertime FROM sensordata_events_fm WHERE registertime IS NOT NULL ORDER BY registertime DESC LIMIT 10;
SELECT register_date FROM sensordata_events_fm WHERE register_date IS NOT NULL ORDER BY register_date DESC LIMIT 10;
SELECT regtime FROM sensordata_events_fm WHERE regtime IS NOT NULL ORDER BY regtime DESC LIMIT 10;
SELECT track_time FROM sensordata_events_fm WHERE track_time IS NOT NULL ORDER BY track_time DESC LIMIT 10;
SELECT getcode_time FROM sensordata_events_fm WHERE getcode_time IS NOT NULL ORDER BY getcode_time DESC LIMIT 10;
SELECT broker_time FROM sensordata_events_fm WHERE broker_time IS NOT NULL ORDER BY broker_time DESC LIMIT 10;
SELECT realname_time FROM sensordata_events_fm WHERE realname_time IS NOT NULL ORDER BY realname_time DESC LIMIT 10;
SELECT event_time FROM sensordata_events_fm WHERE event_time IS NOT NULL ORDER BY event_time DESC LIMIT 10;
SELECT response_time FROM sensordata_events_fm WHERE response_time IS NOT NULL ORDER BY response_time DESC LIMIT 10;
SELECT recv_time FROM sensordata_events_fm WHERE recv_time IS NOT NULL ORDER BY recv_time DESC LIMIT 10;