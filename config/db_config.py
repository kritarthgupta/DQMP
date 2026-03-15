# DB_CONFIG = {
#     # 'host': 'localhost',
#     'host': '192.168.x.xx',
#     'user': 'root',
#     'password': 'xxxxxxxxx#',
#     'database': 'dqmp'
# }
DB_CONFIG = {
    "host": '192.168.x.xx',    # Windows IP (Need to change this when location is changed)
    "user": "dqmp_user",       # or "root" if you allowed it
    "password": "xxxxxxxxx", 
    "database": "dqmp"
}

# WSL is Linux, so mysql -h localhost -u root -p tries to connect to a MySQL server running inside WSL, not the Windows MySQL server.
# By default, you don’t have MySQL server running in WSL, only the client (mysql-client) you just installed.

########################## STEPS:- ##########################
# 1) C:\"Program Files"\MySQL\"MySQL Server 8.0"\bin\mysql.exe -u root -p
# 2) Enter password: *************
# 3) mysql> SELECT user, host FROM mysql.user;
# 4) -- Option A: allow root from any host (not recommended for production)
# CREATE USER 'root'@'%' IDENTIFIED BY 'xxxxxxxxx;
# GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;

# -- Option B: safer, create a new user for WSL only (SELECTED-CURRENTLY)
# CREATE USER 'dqmp_user'@'%' IDENTIFIED BY 'xxxxxxxxx;
# GRANT ALL PRIVILEGES ON dqmp.* TO 'dqmp_user'@'%';
# 5)FLUSH PRIVILEGES;
# 6)DB_CONFIG = {
#     "host": "192.168.x.xx",  # Windows IP
#     "user": "dqmp_user",       # or "root" if you allowed it
#     "password": "xxxxxxxxx", 
#     "database": "dqmp"
# }
