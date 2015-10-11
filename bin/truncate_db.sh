mysql -hrdsjjuvbqjjuvbqout.mysql.rds.aliyuncs.com -P3306 -udongsh  -p5561225 << EOF
use ad_robot_db;
truncate proxy_ip_list;
EOF