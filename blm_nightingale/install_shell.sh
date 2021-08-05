# 1.安装TDengine 和 blm_n9e
wget https://www.taosdata.com/assets-download/TDengine-server-2.1.6.0-beta-Linux-x64.rpm -O TDServer.rpm
rpm -iv TDServer.rpm
# blm_n9e
go build -o blm_n9e
chmod +x blm_n9e
mkdir -p /home/blm/n9e/config
cp blm_n9e /home/blm/n9e/
cp sample/blm_n9e_v5.toml /home/blm/n9e/config/
# 手动执行
# /home/blm/n9e/blm_n9e -config /home/blm/n9e/config/blm_n9e_v5.toml

# 2.安装mysql，root默认密码为1234
yum -y install mariadb*
# 假设机器的/home分区是个SSD的大分区，datadir设置为/home/mysql
# mkdir -p /home/mysql
# chown mysql:mysql /home/mysql
# sed -i '/^datadir/s/^.*$/datadir=\/home\/mysql/g' /etc/my.cnf
# 启动mysql进程
systemctl start mariadb.service
# 将mysql设置为开机自启动
systemctl enable mariadb.service
# 设置mysql root密码
mysql -e "SET PASSWORD FOR 'root'@'localhost' = PASSWORD('1234');"

# 3.安装n9e-server
mkdir -p /opt/n9e
cd /opt/n9e
wget 116.85.64.82/n9e-server-5.0.0-rc3.tar.gz
tar zxvf n9e-server-5.0.0-rc3.tar.gz
mysql -uroot -p1234 < /opt/n9e/server/sql/n9e.sql

cp /opt/n9e/server/etc/service/n9e-server.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable n9e-server
systemctl restart n9e-server
systemctl status n9e-server