import React, { Component } from 'react'
import axios from 'axios';
import { baseUrl } from './config'
import './Job.css'

class Job extends Component {
    constructor(props) {
        super(props);
        this.state = {
            jobs: [],
        };

    }

    componentDidMount() {
        const link = baseUrl + "/jobs/"
        axios.get(link, { params: {} })
            .then(res => {
                console.log(res);
                // console.log(res.data);
                this.setState({ jobs: res.data });
            })
        // console.log('Jobs: ', this.state.jobs)
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
        return (
            <table id='students'>
                <tbody>
                    {this.renderTableData()}
                </tbody>



            </table>
        )
    }
}

export default Job

