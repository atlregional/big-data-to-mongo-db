const mongoose = require('mongoose'),
	Schema = mongoose.Schema;

const modelConfig = require('./model-association-config.json');

module.exports = (collectionName, keys) => {
	const schemaObj = {};

	keys.forEach(key => (schemaObj[key] = { type: String }));

	modelConfig.forEach(model => {
		if (collectionName === model.parent) {
			schemaObj[`${model.table.toLowerCase()}`] = [
				{ type: Schema.Types.ObjectId, ref: `${model.table.toLowerCase()}` }
			];
		}
	});

	// console.log({ collectionName, schemaObj });

	return mongoose.model(collectionName, schemaObj);
};
