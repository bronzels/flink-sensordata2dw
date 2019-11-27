DROP TABLE IF EXISTS sensordata_users_fm;

CREATE TABLE sensordata_users_fm
(
id bigint WITH (primary_key=true),
first_id VARCHAR WITH (nullable = true),
second_id VARCHAR WITH (nullable = true),
mystr_interkafka_ptoffset VARCHAR,
_dwsyncts BIGINT,
"$first_visit_time" bigint WITH (nullable = true),
"$first_referrer" VARCHAR WITH (nullable = true),
"$first_browser_language" VARCHAR WITH (nullable = true),
"$first_referrer_host" VARCHAR WITH (nullable = true),
"$utm_source" VARCHAR WITH (nullable = true),
"$utm_medium" VARCHAR WITH (nullable = true),
"$utm_campaign" VARCHAR WITH (nullable = true),
"$utm_content" VARCHAR WITH (nullable = true),
"$utm_term" VARCHAR WITH (nullable = true),
usertype VARCHAR WITH (nullable = true),
realname VARCHAR WITH (nullable = true),
idcard VARCHAR WITH (nullable = true),
userid VARCHAR WITH (nullable = true),
datatype VARCHAR WITH (nullable = true),
firstusetime double WITH (nullable = true),
userid_ double WITH (nullable = true),
nickname VARCHAR WITH (nullable = true),
type VARCHAR WITH (nullable = true),
platform VARCHAR WITH (nullable = true),
vcode VARCHAR WITH (nullable = true),
"from" VARCHAR WITH (nullable = true),
regtime bigint WITH (nullable = true),
firstusedate bigint WITH (nullable = true),
mobile VARCHAR WITH (nullable = true),
email VARCHAR WITH (nullable = true),
nickname_ VARCHAR WITH (nullable = true),
channel VARCHAR WITH (nullable = true),
type_ VARCHAR WITH (nullable = true),
userid__ double WITH (nullable = true),
"$first_browser_charset" VARCHAR WITH (nullable = true),
"$first_traffic_source_type" VARCHAR WITH (nullable = true),
"$first_search_keyword" VARCHAR WITH (nullable = true),
"$utm_matching_type" VARCHAR WITH (nullable = true),
platform_ VARCHAR WITH (nullable = true),
registertime double WITH (nullable = true),
regvcode double WITH (nullable = true),
href VARCHAR WITH (nullable = true),
platformtype VARCHAR WITH (nullable = true),
realname_time bigint WITH (nullable = true),
realname_ VARCHAR WITH (nullable = true),
devid VARCHAR WITH (nullable = true),
pid VARCHAR WITH (nullable = true),
act_name VARCHAR WITH (nullable = true),
act_source VARCHAR WITH (nullable = true),
store VARCHAR WITH (nullable = true),
quotes_networkbytes double WITH (nullable = true),
jihuoweizhuce_denglu_ bigint WITH (nullable = true),
jihuoweizhuce_lishi_ bigint WITH (nullable = true),
androidjihuoyonghuciriliucun_1881_0927 bigint WITH (nullable = true),
iosjihuociriliucun180801_180927 bigint WITH (nullable = true),
paihangbangqidong bigint WITH (nullable = true),
huiyouquanqidong bigint WITH (nullable = true),
jihuoweizhuce_2018nian9yue_ bigint WITH (nullable = true),
yueappjihuohouciriweiqidongyonghu bigint WITH (nullable = true),
jihuoweizhuce_2018nian8yue_ bigint WITH (nullable = true),
zhuce1 bigint WITH (nullable = true),
jihuoweizhuce_shangzhou_ bigint WITH (nullable = true),
fenxijieguo7 bigint WITH (nullable = true),
weiguanzhuyonghu1001_1125 bigint WITH (nullable = true),
guanzhuyonghu1001_1125 bigint WITH (nullable = true),
zhucedangtianweijiaoyi1001_1125 bigint WITH (nullable = true),
zhucedangtianjiaoyi1001_1125 bigint WITH (nullable = true),
zhuceweijiaoyidingyue bigint WITH (nullable = true),
zhucehoujiaoyidingyue bigint WITH (nullable = true),
zhucehouliushi2018nian11yue bigint WITH (nullable = true),
anzhuo11yuejihuoyonghu bigint WITH (nullable = true),
anzhuo10yuejihuoyonghu bigint WITH (nullable = true),
anzhuo2018nianjihuociyueliucunyonghu bigint WITH (nullable = true),
appzhuceliucunyonghu2018nian bigint WITH (nullable = true),
nianfangwenappdezhuceyonghu bigint WITH (nullable = true),
fenxijieguo20 bigint WITH (nullable = true),
fenxijieguo21 bigint WITH (nullable = true),
fenxijieguo22 bigint WITH (nullable = true),
fenxijieguo23 bigint WITH (nullable = true),
appjin3yueciriliucun_weizhuce bigint WITH (nullable = true),
appjin3yue7riliucun_weizhuce bigint WITH (nullable = true),
webjin1yueciriliucun_weizhuce bigint WITH (nullable = true),
nian2yueappjihuohou7riliushiyonghu bigint WITH (nullable = true),
nian2yueappzhucehou7riliushiyonghu bigint WITH (nullable = true),
monijiaoyibingkaihu_bangding bigint WITH (nullable = true),
monijiaoyiweikaihu_bangding bigint WITH (nullable = true),
zhucehouweiguanzhu bigint WITH (nullable = true),
zhucehouweihudong bigint WITH (nullable = true),
zhuceyonghu2018 bigint WITH (nullable = true),
jihuoweizhuce2018 bigint WITH (nullable = true),
fenxijieguo29 bigint WITH (nullable = true),
fenxijieguo30 bigint WITH (nullable = true),
appfangwenyonghu bigint WITH (nullable = true)
) WITH (
  partition_by_hash_columns = ARRAY['id'],
  partition_by_hash_buckets = 12,
  number_of_replicas = 1
);

