import React, { Component } from 'react'
import axios from 'axios';
import { baseUrl } from './config'

class Job extends Component {

    componentDidMount() {
        const link = baseUrl + "/jobs/"
        axios.get(link, { params: {} })
            .then(res => {
                console.log(res);
                console.log(res.data);
            })
    }
    render() {
        return (
            <table>
                <thead>
                    <tr>
                        <th>Name</th>
                        <th>Job</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Charlie</td>
                        <td>Janitor</td>
                    </tr>
                    <tr>
                        <td>Mac</td>
                        <td>Bouncer</td>
                    </tr>
                    <tr>
                        <td>Dee</td>
                        <td>Aspiring actress</td>
                    </tr>
                    <tr>
                        <td>Dennis</td>
                        <td>Bartender</td>
                    </tr>
                </tbody>
            </table>
        )
    }
}

export default Job

