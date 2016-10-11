import sys, os, datetime, time, logging
TOTAL_DAYS=60


##machine:ubuntu@ip-172-31-16-174
DUMP_CMD = "/app/apache-hive-1.2.1-bin/bin/hive"

def dump(date_str,user_id):
	cmd_str = """%s -e "use opera; select a.label, a.user_id, a.news_entry_id, b.user_profile, b.news_profile from ml_training_impr_click_joined as a join news_running_feature_by_country b on a.dt=b.dt and a.country=b.country and a.user_id=b.user_id and a.request_id=b.request_id and a.news_entry_id=b.news_entry_id where a.dt='%s' and a.country='id' and a.user_id = '%s';" >> /mnt1/chenxj/new_dump/%s_%sid_unf_ffm.feature""" % (DUMP_CMD, date_str,user_id, user_id,date_str)
	os.popen(cmd_str, 'r')
	cmd_str = """%s -e "use opera; select a.label, a.user_id, a.news_entry_id, b.user_profile, b.news_profile from ml_training_impr_click_joined as a join news_running_feature_by_country b on a.dt=b.dt and a.country=b.country and a.user_id=b.user_id and a.request_id=b.request_id and a.news_entry_id=b.news_entry_id where a.dt='%s' and a.country='us' and a.user_id = '%s';" >> /mnt1/chenxj/new_dump/%s_%s_us_unf_ffm.feature""" % (DUMP_CMD, date_str, user_id,user_id,date_str)
	os.popen(cmd_str, 'r')
	logging.info("date:%s, cmd:%s", date_str, cmd_str)

def dump_history_by_user_id(user_id):
	end_date = datetime.datetime.now()
	print end_date
	days = 1 
	while True:
		_date = end_date - datetime.timedelta(days=days+1)
		print _date
		if days > TOTAL_DAYS:
			print 'break now'
			break
		_date_str = _date.strftime('%Y%m%d')
		dump(_date_str,user_id)
		print 'dump now'
		days +=1

def main():
	logging.basicConfig(filename='/mnt1/chenxj/log/dump.log',level=logging.DEBUG)
	logging.info("begin to run")
	dump_history_by_user_id('b7375f1efe756f482cfa564f9285ddc120dc8822')
if __name__ == "__main__":
	main()
