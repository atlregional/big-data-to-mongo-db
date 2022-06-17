  const path = '../extracts/transactions/2020/FULTON-Transactions2020.json'

  const collection = 'PropertyInfo';
  const field = 'ImportParcelID';
  const queryValue = '38293201';

  const json = require(path).filter(record =>
    record[collection].length === 1
  );

  const record = json.find(record =>
    record[collection][0][field] === queryValue
  )