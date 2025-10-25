to_mysql:
	docker exec -it extract_db mysql -u ${MYSQL_USER} -p"${MYSQL_PASSWORD}" ${MYSQL_DATABASE}

to_mysql_root:
	docker exec -it extract_db mysql -u root -p${MYSQL_ROOT_PASSWORD} ${MYSQL_DATABASE}

mysql_create:
	docker exec -it extract_db mysql --local_infile -u ${MYSQL_USER} -p${MYSQL_PASSWORD} ${MYSQL_DATABASE} -e"source /tmp/sql/extract_db/extract_db.sql"