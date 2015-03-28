# Redis RDB local backup script
This python script `redis_backup.py` will create local Redis RDB-snapshot backups using the `bgsave` command to a given directory:

0. Issue a `bgsave` command
1. Wait for `bgsave` to finish successfully
2. Copy the newly created RDB file to the backup directory
3. Verify MD5 checksum
4. Remove old `.rdb` files if the backup directory is too crowded

## Environment Requirement

0. Python 2.7
1. Python modules: `redis`
2. A redis instance running locally. *You should always `rsync` the local backup directory to other servers to maximize the data safety.*

## Running the script
The script can be run with no arguments:

	$ python reids_backup.py
	backup begin @ 2015-03-28 18:30:43.828446
	backup dir:    	/opt/work/RedisBackup/backups
	backup file:   	redis_dump_%Y-%m-%d_%H%M%S
	max backups:   	10
	redis port:    	6379
	bgsave timeout:	60 seconds
	connected to redis server localhost:6379
	redis rdb file path: /opt/src/redis-2.8.19/dump.rdb
	redis bgsave... ok
	backup /opt/work/RedisBackup/backups/redis_dump_2015-03-28_183044(port_6379).rdb created. 18 bytes, checksum ok!
	backup successful! time cost: 0:00:01.005492


If everything goes well, this will create a RDB backup file in the `./backups` directory.