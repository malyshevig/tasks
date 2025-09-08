d='/Users/im/Library/Application Support/Postgres/var-14'
pg_clt=/Applications/Postgres.app/Contents/Versions/14/bin/pg_ctl


$pg_clt -D $d stop
$pg_clt -D $d start
