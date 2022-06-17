const typeDictionary = require('./typeDictionary');
module.exports = (value, type) => {
  switch (typeDictionary[type]) {
    case 'String':
      return `${value}`;
    case 'Number':
      return Number(value);
    case 'Date':
      return new Date(value)
    default:
      return value;
  }
}