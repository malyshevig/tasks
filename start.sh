alias python=/Users/im/imavito/report/.venv/bin/python
num_workers=30

python -m dispatch.dispatcher -p 5000 >./logs/dispatcher0.log 2>&1 &
python -m dispatch.dispatcher -p 5001 >./logs/dispatcher1.log 2>&1 &
python -m dispatch.dispatcher -p 5002 >./logs/dispatcher2.log 2>&1 &
python -m dispatch.dispatcher -p 5003 >./logs/dispatcher3.log 2>&1 &
python -m dispatch.dispatcher -p 5004 >./logs/dispatcher4.log 2>&1 &

python -m dispatch.audit >./logs/audit0.log 2>&1 &
python -m dispatch.audit >./logs/audit1.log 2>&1 &
python -m dispatch.audit >./logs/audit2.log 2>&1 &

for i in $(seq 1 $num_workers);
do
    python -m dispatch.worker -i $i>./logs/worker$i.log 2>&1 &
done


