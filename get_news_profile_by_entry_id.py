import sys, os, datetime, time, logging


##machine:ubuntu@ip-172-31-16-174
DUMP_CMD = "/app/apache-hive-1.2.1-bin/bin/hive"

def get_news_profile_by_entry_id(entry_id):
	cmd_str = """%s -e "use opera_ods; select * from news_profile_all  where news_entry_id='%s' limit 1000;" >> /mnt1/chenxj/entry_id/%s.profile""" % (DUMP_CMD, entry_id,entry_id)
	#cmd_str = """%s -e "use opera; show tables;" >> /mnt1/chenxj/entry_id/%s.profile""" % (DUMP_CMD, entry_id)
	os.popen(cmd_str, 'r')
	print cmd_str



def main():
	logging.basicConfig(filename='/mnt1/chenxj/entry_id/log/dump.log',level=logging.DEBUG)
	logging.info("begin to run")
	get_news_profile_by_entry_id('275a49b0')

if __name__ == "__main__":
    main()
