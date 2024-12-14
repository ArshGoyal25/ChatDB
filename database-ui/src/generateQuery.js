import React, { useState, useEffect } from 'react';
import axios from 'axios';
import './GenerateQuery.css'; // Custom styling

const GenerateQuery = () => {
  const [dbType, setDbType] = useState('mysql');
  const [tableName, setTableName] = useState('');
  const [userInput, setUserInput] = useState('');
  const [generatedQueries, setGeneratedQueries] = useState([]);
  const [statusMessage, setStatusMessage] = useState('');
  const [statusType, setStatusType] = useState(''); // 'success' or 'error'
  const [databaseDetails, setDatabaseDetails] = useState(null);
  const [tableDetails, setTableDetails] = useState(null);
  const [chosenQuery, setChosenQuery] = useState('');
  const [runStatusMessage, setRunStatusMessage] = useState('');
  const [queryResults, setQueryResults] = useState(null);
  const [tableNames, setTableNames] = useState([]); // Holds the table names for selected dbType
  const [recommendedQueries, setRecommendedQueries] = useState([]);
  const [suggestions, setSuggestions] = useState([]);

  // Fetch table names based on selected database type
  useEffect(() => {
    if (dbType) {
      const fetchTableNames = async () => {
        try {
          const response = await axios.get('http://127.0.0.1:5000/api/get_table_names', {
            params: { db_type: dbType },
          });
          setTableNames(response.data.table_names || []);
          setTableName(response.data.table_names[0] || ''); // Set first table as default
        } catch (error) {
          setStatusMessage('Error fetching table names.');
          setStatusType('error');
        }
      };

      fetchTableNames();
    }
  }, [dbType]);

  const handleDbChange = (e) => {
    setDbType(e.target.value);
    setGeneratedQueries([]);
    setRecommendedQueries([]);
    setQueryResults(null);
  };

  const handleTableNameChange = (e) => {
    setTableName(e.target.value);
    setGeneratedQueries([]);
    setRecommendedQueries([]);
    setQueryResults(null);
  };

  const handleUserInputChange = (e) => {
    setUserInput(e.target.value);
  };

  const handleSubmit = async (e) => {
    e.preventDefault();

    if (!userInput) {
      setStatusMessage('Please enter a query prompt.');
      setStatusType('error');
      setGeneratedQueries([]);
      return;
    }
    setRunStatusMessage('');
    setDatabaseDetails(null);
    setTableDetails(null);
    setGeneratedQueries([]);
    setRecommendedQueries([]);
    setQueryResults(null);
    setChosenQuery('');
    setSuggestions([]);
    const requestData = {
      table_name: tableName,
      user_input: userInput,
      db_type: dbType,
    };

    try {
      const response = await axios.post('http://127.0.0.1:5000/api/generate_query', requestData);
      const data = response.data;

      if (data.queries) {
        setGeneratedQueries(data.queries);
        if (data.suggestions) {
          setSuggestions(data.suggestions);
        }
        setStatusMessage('Queries generated successfully.');
        setStatusType('success');
      } else if (data.database_details) {
        setDatabaseDetails(data.database_details);
        setStatusMessage('Database details retrieved successfully.');
        setStatusType('success');
      } else if (data.table_details) {
        setTableDetails(data.table_details);
        setStatusMessage('Table details retrieved successfully.');
        setStatusType('success');
      } else {
        setStatusMessage('Unknown response from the server.');
        setStatusType('error');
      }

      if (data.recommend)
      {
        setRecommendedQueries(data.recommend);
      }


    } catch (error) {
      const readableError = error.response?.data?.error || error.message;
      const suggestions = error.response?.data?.suggestions || [];
      setSuggestions(suggestions);
      setStatusMessage(`Error generating queries: ${parseErrorMessage(readableError)}`);
      setStatusType('error');
    }
  };

  const handleRunQuery = async (query) => {
    setChosenQuery(query);
    setQueryResults(null);
    setRunStatusMessage('');
    const requestData = {
      query: query,
      db_type: dbType,
    };

    try {
      const response = await axios.post('http://127.0.0.1:5000/api/execute_query', requestData);
      if (response.data && response.data.length > 0) {
        setQueryResults(response.data);
      } else {
        setRunStatusMessage('Query executed successfully but returned no results.');
      }
    } catch (error) {
      const readableError = error.response?.data?.error || error.message;
      setRunStatusMessage(`Error executing query: ${parseErrorMessage(readableError)}`);
    }
  };

  const handleClearResults = () => {
    setChosenQuery('');
    setQueryResults(null);
    setRunStatusMessage('');
  };

  const parseErrorMessage = (errorMessage) => {
    // console.log(errorMessage, errorMessage.substring(0, 6))
    
    if (errorMessage === "Please enter a table name in the box above") {
      return "Please select a table to get table details";
    }
    if (errorMessage.includes('SQL execution error')) {
      if (errorMessage.includes('not in GROUP BY clause')) {
        return 'The query is invalid due to incorrect grouping. Check your GROUP BY clause.';
      }
      if (errorMessage.includes('Syntax error')) {
        return 'There is a syntax error in your query.';
      }
      return 'An SQL error occurred. Please check your query.';
    }
    if (errorMessage){
      return errorMessage;
    }
    return 'An unknown error occurred. Please check your inputs or try again.';
  };

  return (
    <div className="generate-query-section">
      <h2>Enter Prompts</h2>
      <form onSubmit={handleSubmit} className="generate-query-form">
        <div>
          <label>Target Database:</label>
          <select value={dbType} onChange={handleDbChange}>
            <option value="mysql">SQL</option>
            <option value="nosql">NoSQL</option>
          </select>
        </div>
        <div>
          <label>Table Name / Collection Name:</label>
          <select value={tableName} onChange={handleTableNameChange}>
            {tableNames.length > 0 ? (
              tableNames.map((name) => (
                <option key={name} value={name}>
                  {name}
                </option>
              ))
            ) : (
              <option value="">No tables available</option>
            )}
          </select>
        </div>
        <div>
          <label>Enter Query Prompt:</label>
          <textarea
            value={userInput}
            onChange={handleUserInputChange}
            placeholder="Describe the query"
            rows="4"
            style={{ width: '100%' }}
          />
        </div>

        {recommendedQueries.length > 0 && (
          <div id="recommended-section">
            <p>You can also try complex queries like:</p>
            <ul>
              {recommendedQueries.map((query, index) => (
                <li key={index}>{query}</li>
              ))}
            </ul>
          </div>
        )}
        <button type="submit">Run</button>
      </form>

      {statusMessage && statusType === 'error' && (
        <div className='error-box'>
          {statusMessage}
        </div>
      )}

      {/* Display database details */}
      {databaseDetails && (
        <div>
          <h3>Database Details:</h3>
          <table border="1" cellPadding="10">
            <thead>
              <tr>
                <th>Table Name</th>
              </tr>
            </thead>
            <tbody>
              {databaseDetails.map((item) => (
                <tr key={item}>
                  <td>{item}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Display table details */}
      {tableDetails && dbType === 'mysql' && (
        <div>
          <h3>Table Details:</h3>
          <table border="1" cellPadding="10">
            <thead>
              <tr>
                <th>Column Name</th>
                <th>Type</th>
              </tr>
            </thead>
            <tbody>
              {Object.entries(tableDetails).map(([columnName, columnType]) => (
                <tr key={columnName}>
                  <td>{columnName}</td>
                  <td>{columnType}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
 {/* Display table details as JSON for NoSQL */}
 {tableDetails && dbType === 'nosql' && (
        <div>
          <h3>Table Details (MongoDB Collection):</h3>
          <pre
            style={{
              backgroundColor: '#f4f4f4',
              padding: '10px',
              borderRadius: '5px',
              overflow: 'auto',
              maxHeight: '400px',
            }}
          >
            {JSON.stringify(tableDetails, null, 2)}
          </pre>
        </div>
      )}
      {/* Display generated queries */}
      {generatedQueries.length > 0 && (
        <div>
          <h3>Example Queries:</h3>
          <ul className="generated-query-list">
            {generatedQueries.map((query, index) => (
              <>
              <li key={index} style={{ marginBottom: '10px' }}>
                <pre
                  style={{
                    whiteSpace: 'pre-wrap',
                    wordWrap: 'break-word',
                    backgroundColor: '#f4f4f4',
                    padding: '10px',
                    borderRadius: '5px',
                    overflowX: 'auto',
                    maxWidth: '100%',
                    lineHeight: '1.5',
                  }}
                >
                  {query}
                </pre>
                <button className="run-button" onClick={() => handleRunQuery(query)}>
                  Run Query
                </button>
              </li>
                <br />
              </>
            ))}
          </ul>
        </div>
      )}

      {/* Display query results */}
      {chosenQuery && (
        <div className="query-results">
          <h3>Run Results</h3>
          <p>
            <strong>Executed Query:</strong> <br />
            <code>{chosenQuery}</code>
          </p>
          {runStatusMessage && (
            <div className={queryResults ? 'success-box' : 'error-box'}>
              {runStatusMessage}
            </div>
          )}
          {queryResults && (
            <div className="results-container">
              {dbType === 'nosql' ? (
                <pre
                  style={{
                    backgroundColor: '#f4f4f4',
                    padding: '10px',
                    borderRadius: '5px',
                    overflow: 'auto',
                    maxHeight: '400px',
                  }}
                >
                  {JSON.stringify(queryResults, null, 2)}
                </pre>
              ) : (
                <table>
                  <thead>
                    <tr>
                      {Object.keys(queryResults[0] || {}).map((key) => (
                        <th key={key}>{key}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {queryResults.map((row, index) => (
                      <tr key={index}>
                        {Object.values(row).map((value, i) => (
                          <td key={i}>{value}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
          <br />
          <button className="clear-button" onClick={handleClearResults}>
            Clear Results
          </button>
        </div>
      )}

    {suggestions.length > 0 && (
        <div className="suggestion-box">
          <h3>Suggested Prompts:</h3>
          <ul>
            {suggestions.map((suggestion, index) => (
              <li key={index} className="suggestion-item">{suggestion}</li>
            ))}
          </ul>
        </div>
      )}
    </div>
  );
};

export default GenerateQuery;
