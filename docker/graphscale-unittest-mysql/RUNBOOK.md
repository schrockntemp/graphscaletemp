# build the image
docker build -t graphscale-unittest-mysql-image  .

# run the image (wait for service to start [see below])
docker run --name graphscale-unittest-mysql-container -p 3306:3306  -d graphscale-unittest-mysql-image 

# tail the logs and makes sure mysql has started up
docker logs -f graphscale-unittest-mysql-container

# connect directly from host machine to db
mysql -uroot --password=passwordfortest

# connect directly to the unittest db from the host
mysql --user=magnus --password=magnus graphscale-unittest

# connect from another docker temporary instance
docker run -it --link graphscale-unittest-mysql-container:mysql --rm mysql sh -c 'exec mysql -h"$MYSQL_PORT_3306_TCP_ADDR" -P"$MYSQL_PORT_3306_TCP_PORT" -uroot -p"$MYSQL_ENV_MYSQL_ROOT_PASSWORD"'

