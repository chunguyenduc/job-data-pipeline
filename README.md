
# **Job ETL Pipeline**

<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
      <li><a href="#motivation">Motivation</a></li>
      <li><a href="#built-with">Built With</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>



<!-- ABOUT THE PROJECT -->
## Motivation
I want to make a beginner data engineering project, also want to look for a new job. So i make this ETL pipeline that crawl data from job search website, do some transform, store data in data warehouse and load data to visualization tool.

## Built With

* [Airflow](https://airflow.apache.org/)
* [Hadoop (HDFS)](https://hadoop.apache.org/)
* [Spark](https://spark.apache.org/)
* [Hive](https://hive.apache.org/)
* [Superset](https://superset.apache.org/)
* [Docker](https://www.docker.com/)
* [Prometheus](https://prometheus.io/) & [Grafana](https://grafana.com)


## Architecture
![Data pipeline design](media/job_etl.jpg)
1. Extract data using Scrapy
2. Load data into HDFS
3. Use Spark to transform, remove unnecessary data
4. Load data to Hive
5. Create dashboard, write queries in Superset
6. Orchestrate with Airflow in Docker
7. Use Prometheus and Grafana to monitor resources

## Usage


- Clone and cd into the project directory.

```bash
git clone https://github.com/chunguyenduc/Job-ETL-Pipeline.git
cd Job-ETL-Pipeline
```

- Use  `docker compose` to start. The pipeline has a lot of components so this may take a while
```
docker compose up -d
```

- Go to Airflow webserver on [localhost:8080](http://localhost:8080) and turn on our DAG

![Job ETl Pipeline DAG](media/jot_etl_pipeline_dag.png)

- Our DAG has four tasks as step 1 to 4 in [Architecture](#architecture). The DAG runs every 15 minutes and crawl the first page of [ITviec](https://itviec.com/it-jobs?page=1&query=&source=search_job)

<!-- ![Job ETl Pipeline Task](media/job_etl_pipeline_task.png) -->
| ![Job ETl Pipeline Task](media/job_etl_pipeline_task.png) | 
|:--:| 
| *Tasks* |

| ![ITviec website](media/itviec_website.png) | 
|:--:| 
| *ITviec Website* |

- Before running Hive, you need to create the /tmp folder and a separate Hive folder in HDFS:

```
docker exec -it namenode /bin/bash
hadoop fs -mkdir /tmp 
hadoop fs -mkdir /user/hive/warehouse
hadoop fs -chmod g+w /tmp 
hadoop fs -chmod g+w /user/hive/warehouse
```

- Once the DAG is done running, you can query data or create dashboard in Superset like this:
![Job-ETL-Dashboard](media/job_etl_dashboard.jpg)


<!-- ROADMAP -->
## Roadmap

- [x] Add Changelog
- [x] Add back to top links
- [ ] Add Additional Templates w/ Examples
- [ ] Add "components" document to easily copy & paste sections of the readme
- [ ] Multi-language Support
    - [ ] Chinese
    - [ ] Spanish

See the [open issues](https://github.com/othneildrew/Best-README-Template/issues) for a full list of proposed features (and known issues).

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".
Don't forget to give the project a star! Thanks again!

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- LICENSE -->
## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- CONTACT -->
## Contact

Your Name - [@your_twitter](https://twitter.com/your_username) - email@example.com

Project Link: [https://github.com/your_username/repo_name](https://github.com/your_username/repo_name)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

Use this space to list resources you find helpful and would like to give credit to. I've included a few of my favorites to kick things off!

* [Choose an Open Source License](https://choosealicense.com)
* [GitHub Emoji Cheat Sheet](https://www.webpagefx.com/tools/emoji-cheat-sheet)
* [Malven's Flexbox Cheatsheet](https://flexbox.malven.co/)
* [Malven's Grid Cheatsheet](https://grid.malven.co/)
* [Img Shields](https://shields.io)
* [GitHub Pages](https://pages.github.com)
* [Font Awesome](https://fontawesome.com)
* [React Icons](https://react-icons.github.io/react-icons/search)

<p align="right">(<a href="#readme-top">back to top</a>)</p>



<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/othneildrew/Best-README-Template.svg?style=for-the-badge
[contributors-url]: https://github.com/othneildrew/Best-README-Template/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/othneildrew/Best-README-Template.svg?style=for-the-badge
[forks-url]: https://github.com/othneildrew/Best-README-Template/network/members
[stars-shield]: https://img.shields.io/github/stars/othneildrew/Best-README-Template.svg?style=for-the-badge
[stars-url]: https://github.com/othneildrew/Best-README-Template/stargazers
[issues-shield]: https://img.shields.io/github/issues/othneildrew/Best-README-Template.svg?style=for-the-badge
[issues-url]: https://github.com/othneildrew/Best-README-Template/issues
[license-shield]: https://img.shields.io/github/license/othneildrew/Best-README-Template.svg?style=for-the-badge
[license-url]: https://github.com/othneildrew/Best-README-Template/blob/master/LICENSE.txt
[linkedin-shield]: https://img.shields.io/badge/-LinkedIn-black.svg?style=for-the-badge&logo=linkedin&colorB=555
[linkedin-url]: https://linkedin.com/in/othneildrew
[product-screenshot]: images/screenshot.png
[Airflow]: https://upload.wikimedia.org/wikipedia/commons/thumb/d/de/AirflowLogo.png/1200px-AirflowLogo.png
[Next-url]: https://nextjs.org/
[React.js]: https://img.shields.io/badge/React-20232A?style=for-the-badge&logo=react&logoColor=61DAFB
[React-url]: https://reactjs.org/
[Vue.js]: https://img.shields.io/badge/Vue.js-35495E?style=for-the-badge&logo=vuedotjs&logoColor=4FC08D
[Vue-url]: https://vuejs.org/
[Angular.io]: https://img.shields.io/badge/Angular-DD0031?style=for-the-badge&logo=angular&logoColor=white
[Angular-url]: https://angular.io/
[Svelte.dev]: https://img.shields.io/badge/Svelte-4A4A55?style=for-the-badge&logo=svelte&logoColor=FF3E00
[Svelte-url]: https://svelte.dev/
[Laravel.com]: https://img.shields.io/badge/Laravel-FF2D20?style=for-the-badge&logo=laravel&logoColor=white
[Laravel-url]: https://laravel.com
[Bootstrap.com]: https://img.shields.io/badge/Bootstrap-563D7C?style=for-the-badge&logo=bootstrap&logoColor=white
[Bootstrap-url]: https://getbootstrap.com
[JQuery.com]: https://img.shields.io/badge/jQuery-0769AD?style=for-the-badge&logo=jquery&logoColor=white
[JQuery-url]: https://jquery.com 
