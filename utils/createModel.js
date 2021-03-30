const mongoose = require('mongoose'),
  Schema = mongoose.Schema;


module.exports = async (collectionName, keys, modelConfig, joinField) => {
  const schemaObj = {};
  // console.log(keys);
  // const modelConfig = require('./model-association-config.json');


  keys.forEach(key => schemaObj[key] = { type: String });

  // modelConfig.forEach(model => {
  // 	if (collectionName === model.parent) {
  // 		schemaObj[model.table] = [
  // 			{ type: Object, ref: model.table }
  // 		];
  // 	}
  // });

  // modelConfig.forEach(model => {
  // 	if (collectionName === model.parent) {
  // 		schemaObj[model.table] = { type: String || Array };
  // 	}
  // });

  // console.log({ collectionName, schemaObj });
  let schema = new Schema(schemaObj, {
    collection: collectionName,
    toJSON: { virtuals: true }
  });

  modelConfig.forEach(model => {
    if (collectionName !== model.table && !model.exclude) {
      schema.virtual(model.table, {
        ref: model.table,
        localField: joinField,
        foreignField: joinField,
        justOne: false
      });
    }
  });

  // collectionName === 'Main' ? console.log(schema) : null;



  return mongoose.model(collectionName, schema);
};
