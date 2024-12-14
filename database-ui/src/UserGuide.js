// UserGuide.jsx
import React from 'react';
import './UserGuide.css';

const UserGuide = () => {
  return (
    <div className="user-guide">
      <header className="guide-header">
        <h1>ChatDB User Guide</h1>
        <p>Your complete guide to uploading files, generating queries, and running database operations with ChatDB.</p>
      </header>

      {/* File Upload Section */}
      <section className="guide-section">
        <h2>File Upload</h2>
        <p style={{
              textAlign: 'center'}}>
          The File Upload feature is your starting point. Use this to upload your database files for processing and query generation. Follow these steps:
        </p>
        <ol className="steps-list">
          <li>Click the <strong>Upload File</strong> button in the File Upload section.</li>
          <li>Choose a supported database file (e.g., <i>.xlsx</i> or <i>.xls</i> for SQL or <i>.json</i> for MongoDB).</li>
          <li>Wait for a success message confirming the upload</li>
          <li>If there’s an issue, an error message will guide you to fix it.</li>
        </ol>
        <div className="info-box">
          <strong>Tip:</strong> Ensure your file is in the correct format and contains valid data for seamless upload.
        </div>
      </section>

      {/* Query Generation Section */}
      <section className="guide-section">
        <h2>2. Query Generation</h2>
        <p>
          Generate queries and interact with your database through these simple steps:
        </p>

        {/* Database Selection */}
        <h3 style={{
              textAlign: 'left'}}>Step 1: Choose Database Type and Table</h3>
        <p style={{
              textAlign: 'left'}}>
          Begin by selecting your database type:
        </p>
        <ul style={{
              textAlign: 'left'}} className="bullet-list">
          <li><strong>SQL:</strong> For relational databases (MySQL)</li>
          <li><strong>NoSQL:</strong> For document-oriented databases(MongoDB)</li>
          </ul>

        <p style={{
              textAlign: 'left'}}>Then, pick a table (SQL) or collection (MongoDB) from the dropdown menu</p>

        {/* Describe Table/Collection */}
        <h3 style={{
              textAlign: 'left'}}>Step 2: Describe Table/Collection</h3>
        <p style={{
              textAlign: 'left'}}>
          Use the "Describe" feature to view database structure:
        </p>
        <ul style={{
              textAlign: 'left'}} className="bullet-list"></ul>
          <li style={{
              textAlign: 'left'}}><strong>For SQL:</strong> Displays column names and data types in a tabular format. A sample query is <i>Describe table</i></li>

          <li style={{
              textAlign: 'left'}}><strong>For MongoDB:</strong> Shows the document structure in JSON format. A sample query is <i>Describe collection</i></li>
        

        {/* Generating Queries */}
        <h3 style={{
              textAlign: 'left'}}>Step 3: Generate Example Queries</h3>
        <p style={{
              textAlign: 'left'}}>Input a prompt in the query box to generate examples:</p>
        <ul className="bullet-list" style={{
              textAlign: 'left'}}>
          <li>
            <strong>Generic Prompt:</strong> Typing <i>example query</i> generates three sample queries.
          </li>
          <li>
            <strong>Specific Prompt:</strong> Enter details like <i>example query with GROUP BY</i> for tailored queries.
          </li>
        </ul>

        {/* Running Queries */}
        <h3 style={{
              textAlign: 'left'}}>Step 4: Run Queries</h3>
        <p style={{
              textAlign: 'left'}}> 
          Click the "Run" button next to any generated query to execute it. Results are displayed based on the database type:
        </p>
        <ul style={{
              textAlign: 'left'}} className="bullet-list">
          <li><strong>SQL:</strong> Results appear in a table format.</li>
          <li><strong>NoSQL:</strong> Results are displayed as JSON data.</li>
        </ul>
        <div className="info-box">
          <strong>Tip:</strong> If no results are returned, it means the query executed successfully but retrieved no data.
        </div>
      </section>
    </div>
  );
};

export default UserGuide;
