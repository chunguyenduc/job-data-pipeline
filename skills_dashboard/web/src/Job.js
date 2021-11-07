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
                // console.log(res);
                console.log(res.data);
                const totalPages = res.data.count / params["page_size"]
                this.setState({
                    jobs: res.data.results,
                    totalPages: totalPages,
                });
            })
            .catch((e) => {
                console.log(e);
            });

        // TutorialDataService.getAll(params)
        //     .then((response) => {
        //         const { tutorials, totalPages } = response.data;

        //         this.setState({
        //             tutorials: tutorials,
        //             count: totalPages,
        //         });
        //         console.log(response.data);
        //     })
        //     .catch((e) => {
        //         console.log(e);
        //     });
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
        // const link = baseUrl + "/jobs/"
        // axios.get(link, { params: {} })
        //     .then(res => {
        //         console.log(res.data);
        //         // console.log(res.data);
        //         this.setState({ jobs: res.data.results });
        //     })
        // console.log('Jobs: ', this.state.jobs)
        this.retrieveJobs();
    }

    renderTableData() {
        return this.state.jobs.map((job, index) => {
            const { title, city, company, url, created_at } = job
            // console.log(typeof(created_at))
            // var temp = new Date();
            // temp.setTime(created_at)
            var temp = Date.parse(created_at)
            console.log(typeof (temp))
            var a = new Date(temp);
            var months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
            var year = a.getFullYear();
            var month = months[a.getMonth()];
            var date = a.getDate();
            var hour = "0" + a.getHours();
            var min = "0" + a.getMinutes();
            // var sec = "0" + a.getSeconds();
            var time = date + ' ' + month + ' ' + year + ' ' + hour.substr(-2) + ':' + min.substr(-2);




            return (
                <tr>
                    <td><a href={url} target="_blank" rel="noreferrer">{title}</a></td>
                    <td>{city}</td>
                    <td>{company}</td>
                    <td>{time}</td>
                </tr>
            )
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
                <table id='students'>
                    <tbody>
                        {this.renderTableData()}
                    </tbody>
                </table>
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
        )
    }
}

export default Job

