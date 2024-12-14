import React, { useState, useEffect, useRef } from 'react';
import axios from 'axios';
import './FileUpload.css'; // Import styles for success and error boxes

const FileUpload = () => {
  const [file, setFile] = useState(null);
  const [targetDb, setTargetDb] = useState('sql');
  const [uploadStatusMessage, setUploadStatusMessage] = useState('');
  const [uploadStatusType, setUploadStatusType] = useState(''); // 'error' or 'success'
  const [tableName, setTableName] = useState('');
  const fileInputRef = useRef(null); // Ref for the file input field

  const handleFileChange = (e) => {
    setFile(e.target.files[0]);
  };

  const handleDbChange = (e) => {
    setTargetDb(e.target.value);
  };

  const handleTableNameChange = (e) => {
    setTableName(e.target.value);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!file) {
      setUploadStatusMessage('Please select a file to upload.');
      setUploadStatusType('error');
      return;
    }

    const formData = new FormData();
    formData.append('file', file);
    formData.append('target_db', targetDb);
    if (tableName) {
      formData.append('table_name', tableName);
    }

    try {
      const response = await axios.post('http://127.0.0.1:5000/api/insert', formData);
      setUploadStatusMessage('File uploaded successfully.');
      setUploadStatusType('success');

      // Clear success message and reset the form after 10 seconds
      setTimeout(() => {
        setUploadStatusMessage('');
        setUploadStatusType('');
        resetForm(); // Reset file input and other fields
      }, 5000);
    } catch (error) {
      const readableError = error.response?.data?.error || error.message;
      setUploadStatusMessage(`Error uploading file: ${parseErrorMessage(readableError)}`);
      setUploadStatusType('error');
    }
  };

  const parseErrorMessage = (errorMessage) => {
    if (errorMessage.includes('File format not supported')) {
      return 'The file format is not supported. Please upload a valid Excel file.';
    }
    return 'An error occurred while uploading the file. Please try again.';
  };

  const resetForm = () => {
    setFile(null);
    setTableName('');
    if (fileInputRef.current) {
      fileInputRef.current.value = ''; // Reset file input field
    }
  };

  return (
    <div style={{ margin: '20px' }}>
      <h2>Upload File</h2>
      <form onSubmit={handleSubmit}>
        <div>
          <label>Select File:</label>
          <input
            type="file"
            ref={fileInputRef} // Attach ref to the file input
            onChange={handleFileChange}
            accept=".xlsx, .xls, .json"
          />
        </div>
        <div>
          <label>Target Database:</label>
          <select value={targetDb} onChange={handleDbChange}>
            <option value="sql">SQL</option>
            <option value="nosql">NoSQL</option>
          </select>
        </div>
        <div>
          <label>Table / Collection Name (Optional):</label>
          <input
            type="text"
            value={tableName}
            onChange={handleTableNameChange}
            placeholder="Enter table name"
          />
        </div>
        <button type="submit">Upload</button>
      </form>

      {/* Display success or error messages */}
      {uploadStatusMessage && (
        <div className={uploadStatusType === 'error' ? 'error-box' : 'success-box'}>
          {uploadStatusMessage}
        </div>
      )}
    </div>
  );
};

export default FileUpload;
