import React from 'react';

const DisplayQueryResults = ({ results, query, status }) => {
  // If there is an error, show the error message and stop rendering the rest
  if (status.startsWith('Error')) {
    return (
      <div>
        <h2>Query Execution</h2>
        <p>Executing query: {query}</p>
        <p>Error executing query</p>
      </div>
    );
  }

  return (
    <div>
      <h2>Query Execution</h2>
      <p>Query: {query}</p>

      {/* If no results, display a message */}
      {(!results || results.length === 0) && <p>No results to display.</p>}

      {/* If there are results, display them in a table */}
      {results && results.length > 0 && (
        <div>
          <h4>Results:</h4>
          <table border="1" cellPadding="10">
            <thead>
              <tr>
                {Object.keys(results[0]).map((key) => (
                  <th key={key}>{key}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {results.map((result, index) => (
                <tr key={index}>
                  {Object.values(result).map((value, i) => (
                    <td key={i}>{value}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default DisplayQueryResults;
