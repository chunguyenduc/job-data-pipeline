import './App.css';
import React from 'react';
import Job from './Job';
import {
  BrowserRouter, Routes,
  Route,
} from "react-router-dom";

class App extends React.Component {
  // render()
  // {
  // return (
  //   <Router>
  //     <Routes>
  //       <Route path="/" component={Job} />
  //     </Routes>


  //   </Router>

  // );
  render() {
    return (
      <BrowserRouter>

			<Routes>
				<hr />
				<div className="main-route-place">

				<Route path="/" element={<Job />} />

				</div>
			</Routes>
			</BrowserRouter>
    );
  }

  // }
  // return null
}

export default App;
