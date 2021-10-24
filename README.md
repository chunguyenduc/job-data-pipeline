docker exec -it skillsdashboard_db_1 bash  
scrapy crawl skills  
docker exec -t -i skillsdashboard_web_1 /bin/bash  
