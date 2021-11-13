import React, { Component } from 'react'
import axios from 'axios';
import { baseUrl } from './config'
import './Job.css'
import Pagination from "@material-ui/lab/Pagination";


class Job extends Component {
    constructor(props) {
        super(props);
        this.onChangeSearchTitle = this.onChangeSearchTitle.bind(this);
        this.retrieveJobs = this.retrieveJobs.bind(this);
        this.handlePageChange = this.handlePageChange.bind(this);
        this.handlePageSizeChange = this.handlePageSizeChange.bind(this);
        this.state = {
            jobs: [],
            searchTitle: "",
            totalPages: 0,
            page: 1,
            pageSize: 10,
        };

    }

    onChangeSearchTitle(e) {
        const searchTitle = e.target.value;

        this.setState({
            searchTitle: searchTitle,
        });
    }


    chosePage = (event) => {
        this.setState({
            currentPage: Number(event.target.id)
        });
    }

    getRequestParams(searchTitle, page, pageSize) {
        let params = {};

        if (searchTitle) {
            params["title"] = searchTitle;
        }

        if (page) {
            params["page"] = page;
        }

        if (pageSize) {
            params["page_size"] = pageSize;
        }

        return params;
    }

    retrieveJobs() {
        const { searchTitle, page, pageSize } = this.state;
        const params = this.getRequestParams(searchTitle, page, pageSize);
        const link = baseUrl + "/jobs/"
        axios.get(link, { params })
            .then(res => {
                console.log(res.data);
                const totalPages = Math.floor(res.data.count / params["page_size"]) > 10 ? 10 : Math.floor(res.data.count / params["page_size"])
                this.setState({
                    jobs: res.data.results,
                    totalPages: totalPages,
                });
            })
            .catch((e) => {
                console.log(e);
            });

    }
    handlePageChange(event, value) {
        this.setState(
            {
                page: value,
            },
            () => {
                this.retrieveJobs();
            }
        );
    }

    handlePageSizeChange(event) {
        this.setState(
            {
                pageSize: event.target.value,
                page: 1
            },
            () => {
                this.retrieveJobs();
            }
        );
    }



    componentDidMount() {
        this.retrieveJobs();
    }

    renderTableData() {
        return this.state.jobs.map((job, index) => {
            const { title, city, company, url, created_at, skills, site } = job
            const listSkills = skills.map((s) => ' ' + s).join();
            var temp = Date.parse(created_at)
            console.log(typeof (temp))
            var a = new Date(temp);
            console.log(a.getTimezoneOffset() / 60)
            console.log(a.toLocaleString())
            // var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            var year = a.getFullYear();
            // var month = months[a.getMonth()];
            var month = a.getMonth()+1;

            var date = a.getDate();
            var hour = "0" + a.getHours();
            var min = "0" + a.getMinutes();
            // var sec = "0" + a.getSeconds();
            var time = date + '/' + month + '/' + year + ' ' + hour.substr(-2) + ':' + min.substr(-2);




            return (
                <tr>
                    <td><a id="title" href={url} target="_blank" rel="noreferrer">{title}</a></td>
                    <td id="city">{city}</td>
                    <td>{company}</td>
                    <td>{listSkills}</td>
                    <td>{time}</td>
                    <td>{site}</td>
                </tr>
            )
        })
    }

    renderTableHeader() {
        let header = ['TITLE', 'CITY', 'COMPANY', 'TAG', 'POSTED TIME', 'SITE']
        return header.map((key, index) => {
            return <th id="student" key={index}>{key.toUpperCase()}</th>
        })
    }



    render() {
        const {
            jobs,
            searchTitle,
            totalPages,
            page,
            pageSize,
        } = this.state;
        return (
            <div>

                <div class="jumbotron jumbotron-fluid text-center landing-text">
                    <div class="container">
                        <h1 class="display-4">Machine Learning Contests</h1>
                        <p class="lead">
                        Discover ongoing machine learning and data science competitions.

                        <br></br>
                        <a href="https://kaggle.com">Kaggle</a>, <a href="https://www.drivendata.org">DrivenData</a>, <a href="https://aicrowd.com">AIcrowd</a>, <a href="https://zindi.africa">Zindi</a>, and other platforms.
                        <br></br>
                        Sign up to the mailing list for updates.
                        </p>
                        {/* <iframe src="https://mlcontests.substack.com/embed" style="border:0px; background:white;" scrolling="no" width="100%" height="120" frameborder="0"></iframe> */}
                    </div>

                </div>
                <div>
                    <table id='students'>
                        <thead>
                            {this.renderTableHeader()}
                        </thead>
                        <tbody>
                            {this.renderTableData()}
                        </tbody>
                    </table>
                </div>
                <br />
                <div>
                    <Pagination
                        className="my-3"
                        count={totalPages}
                        page={page}
                        siblingCount={1}
                        boundaryCount={1}
                        variant="outlined"
                        shape="rounded"
                        onChange={this.handlePageChange}
                    />
                </div>

            </div>
        )
    }
}

export default Job

