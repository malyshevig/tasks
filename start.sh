
python -m dispatch.dispatcher -p 5000 >./logs/dispatcher0.log 2>&1 &
python -m dispatch.dispatcher -p 5001 >./logs/dispatcher1.log 2>&1 &
python -m dispatch.dispatcher -p 5002 >./logs/dispatcher2.log 2>&1 &
python -m dispatch.dispatcher -p 5003 >./logs/dispatcher3.log 2>&1 &
python -m dispatch.dispatcher -p 5005 >./logs/dispatcher4.log 2>&1 &

python -m dispatch.audit >./logs/audit0.log 2>&1 &
python -m dispatch.audit >./logs/audit1.log 2>&1 &
python -m dispatch.audit >./logs/audit2.log 2>&1 &

python -m dispatch.worker -i 21 >./logs/worker21.log 2>&1 &
python -m dispatch.worker -i 22 >./logs/worker22.log 2>&1 &
python -m dispatch.worker -i 23 >./logs/worker23.log 2>&1 &


