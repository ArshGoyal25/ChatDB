import React from 'react';
import { BrowserRouter as Router, Routes, Route, Link } from 'react-router-dom';
import FileUpload from './fileUpload';
import GenerateQuery from './generateQuery';
import UserGuide from './UserGuide';
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <header className="App-header">
          <h1>ChatDB</h1>
          <nav>
            <Link to="/" style={{ marginRight: '10px', color: '#fff' }}>Home</Link>
            <Link to="/user-guide" style={{ color: '#fff' }}>User Guide</Link>
          </nav>
        </header>
        <main>
          <Routes>
            <Route
              path="/"
              element={
                <>
                  <section className="feature-section">
                    <FileUpload />
                  </section>
                  <section className="feature-section">
                    <GenerateQuery />
                  </section>
                </>
              }
            />
            <Route path="/user-guide" element={<UserGuide />} />
          </Routes>
        </main>
      </div>
    </Router>
  );
}

export default App;
