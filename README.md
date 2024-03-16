# DE_miniproject2

## to install requirments in ubuntu:
`pip install -r requirements.txt `

## to install crontab and nano:
`sudo apt-get install cron`
`sudo apt-get install nano`

## to add get_data.py to schedules:
`crontab -e`

## to start cron service:
`service cron start`

## to run consumers:
`python3 Consumer1.py`

## to craete a topic:
`kafka-topics --create --topic users_info --bootstrap-server broker:29092 --partitions 1 --replication-factor 1`
