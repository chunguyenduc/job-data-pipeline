docker exec -it skillsdashboard_db_1 bash
scrapy crawl skills
docker exec -t -i skillsdashboard_api_1 /bin/bash
docker exec -it skillsdashboard_crawl_1 bash
docker-compose run crawl curl http://127.0.0.1:6800/schedule.json -d project=default -d spider=skills
