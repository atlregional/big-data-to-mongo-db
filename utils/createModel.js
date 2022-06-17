const mongoose = require('mongoose');
const typeDictionary = require('./typeDictionary');
Schema = mongoose.Schema;



module.exports = (collectionName, fieldInfo, modelConfig, joinField) => {
  const schemaObj = {};

  Object.entries(fieldInfo).forEach(([key, type]) => schemaObj[key] = { type: typeDictionary[type] });

  let schema = new Schema(schemaObj, {
    collection: collectionName,
    toJSON: { virtuals: true }
  });

  modelConfig.forEach(model => {
    if (collectionName !== model.table && model.include) {
      schema.virtual(model.table, {
        ref: model.table,
        localField: joinField,
        foreignField: joinField,
        justOne: false
      });
    }
  });

  return mongoose.model(collectionName, schema);
};
