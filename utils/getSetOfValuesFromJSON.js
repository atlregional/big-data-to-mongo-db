module.exports = async ({county, collection, field, years}) => {

  const list = [];

  const asyncIterator = async => { 
    console.log('Async iterator started')  

    years.forEach(year => {
      console.log('getting values for', year)
    // iterator.counties.forEach(county => {
      const path = `../extracts/transactions/${year}/${county}-Transactions${year}.json`
      const json = require(path).filter(record =>
        record[collection].length === 1
      );

      json.forEach(record => {
          const value = record[collection][0][field];
          if (!list.includes(value)) {
            list.push(value)
          }
      })
    // })
  })}

  await asyncIterator();

  console.log('Done extracting unique value list for', county)

  console.log(list.length);
  return list;
};

