# NDBC - Node Database Connector

NDBC is a lightweight Node.js library designed to simplify database interactions for MySQL and MSSQL databases. It provides a clean and flexible interface for performing queries, retrieving data, and updating records with built-in support for SQL joins, parameterized queries, and data type parsing (e.g., BLOB and datetime handling). This library abstracts the complexity of raw SQL queries while ensuring robust error handling and database schema validation.

## Features

- **Database Support**: Compatible with MySQL and MSSQL databases.
- **Dynamic Query Building**: Construct complex SQL queries with support for joins (INNER, LEFT, RIGHT), WHERE clauses, ORDER BY, and LIMIT.
- **Data Validation**: Automatically validates columns and data types against the database schema to prevent SQL injection and errors.
- **BLOB and Datetime Handling**: Converts BLOB data to base64 and formats datetime fields using the `moment` library.
- **Insert and Update Operations**: Intelligently determines whether to perform an INSERT or UPDATE based on primary key presence and WHERE conditions.
- **Custom Query Execution**: Allows execution of raw SQL queries with parameterized inputs for flexibility.
- **Error Handling**: Comprehensive error reporting for connection issues, invalid columns, and data type mismatches.

## Installation

To install NDBC, use npm with the scoped package name:

```bash
npm install @rjaskonis/ndbc
```

Ensure you have the required dependencies installed:

```bash
npm install lodash moment
```

Additionally, you need the appropriate database driver:
- For MySQL: `npm install mysql`
- For MSSQL: `npm install mssql`

## Usage

### Initialization

First, initialize the library with your database configuration and driver:

```javascript
const ndbc = require('@rjaskonis/ndbc');
const mysql = require('mysql'); // or 'mssql' for MSSQL

const dbConfig = {
  host: 'localhost',
  user: 'your_user',
  password: 'your_password',
  database: 'your_database',
  timeout: 10000 // Optional timeout in milliseconds
};

const db = ndbc({
  base: mysql, // or require('mssql') for MSSQL
  databaseOptions: dbConfig
});
```

### Retrieving Data

Use the `table` method to interact with a specific table and the `getData` method to fetch records. You can specify columns, joins, WHERE conditions, ORDER BY, and LIMIT:

```javascript
const users = db.table('users');

users.getData({
  columns: ['id', 'name', 'email'],
  joins: [
    {
      table: { name: 'orders', alias: 'o', columns: ['order_id', 'amount'] },
      type: 'LEFT',
      on: [
        { column: 'user_id', foreignColumn: 'id' }
      ]
    }
  ],
  where: { 'id': 1, 'name NOT': 'John Doe' },
  order: ['name DESC'],
  limit: 10,
  datetimeFormat: 'MM/DD/YYYY HH:mm' // Optional datetime format
})
.then(results => {
  console.log(results);
})
.catch(error => {
  console.error(error);
});
```

### Inserting or Updating Data

The `setData` method handles both INSERT and UPDATE operations, automatically determining the operation based on primary key presence or WHERE conditions:

```javascript
users.setData(
  {
    id: 1, // If primary key exists, attempts UPDATE; otherwise, INSERT
    name: 'Jane Doe',
    email: 'jane@example.com'
  },
  {
    where: { 'id': 1 } // Optional for UPDATE
  }
)
.then(result => {
  console.log('Affected ID:', result.affectedId);
})
.catch(error => {
  console.error(error);
});
```

### Executing Custom Queries

For raw SQL queries, use the `query` method with parameterized inputs:

```javascript
db.query('SELECT * FROM users WHERE id = {userId}')
  .execute({ userId: 1 })
  .then(results => {
    console.log(results);
  })
  .catch(error => {
    console.error(error);
  });
```

## Error Handling

NDBC validates inputs and schema to prevent common errors:
- **Invalid Columns**: Columns not present in the table schema are removed.
- **Data Type Mismatches**: Ensures numeric fields are valid numbers and VARCHAR fields respect length constraints.
- **Required Fields**: Checks for missing required (non-nullable) fields.
- **SQL Injection Prevention**: Uses parameterized queries for safe data handling.

Errors are returned as rejected promises with descriptive messages, such as SQL errors or validation issues (e.g., `{ column: 'name', issue: 'required' }`).

## Dependencies

- `lodash`: For array and object manipulation.
- `moment`: For datetime parsing and formatting.
- `mysql` or `mssql`: Database drivers for MySQL or MSSQL.

## Contributing

Contributions are welcome! Please fork the repository and submit a pull request with your changes. Ensure your code follows the existing style and includes appropriate tests.

1. Fork the repository: `https://github.com/rjaskonis/ndbc`
2. Create a feature branch: `git checkout -b feature/your-feature`
3. Commit your changes: `git commit -m "Add your feature"`
4. Push to the branch: `git push origin feature/your-feature`
5. Open a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## Contact

For questions or issues, please open an issue on the GitHub repository: [https://github.com/rjaskonis/ndbc](https://github.com/rjaskonis/ndbc).
