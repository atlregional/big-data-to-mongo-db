const fs = require('fs');
const es = require('event-stream');
const mongoose = require('mongoose');

const createModel = require('./utils/createModel');
const modelConfig = require('./utils/model-association-config.json');
const { config } = require('process');

// const databaseName = process.argv[3];
// const manifest = process.argv[2];

const databaseName = 'ztraxAssessment';
const manifest = 'ZTRAX-Assessment';

const MONGODB_URI = process.env.MONGODB_URI || `mongodb://localhost/${databaseName}`;

mongoose.connect(MONGODB_URI, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
	useFindAndModify: false
}).catch(err => {
	console.log(err);
	process.exit(1);
});

const getCollectionKeysFromCSV = tableName => {
	const arr = fs
		.readFileSync(`./manifest/${manifest}.csv`, 'utf-8')
		.split('\r\n')
		.filter(str => str.split(',')[0] === tableName);

	let keys = [];

	for (let i = 0; i < arr.length; i++) {
		keys.push({
			key: arr[i].split(',')[1],
			sorter: arr[i].split(',')[2]
		});
	}

	return keys.sort((a, b) => a.sorter - b.sorter).map(item => item.key);
};

const insertData = () => {
	// Adding file property to objs in config array
	const configArr = modelConfig.map(obj => {
		return {
			table: obj.table,
			parent: obj.parent,
			file: `${obj.table}.txt`
		};
	});

	configArr.forEach(obj => {
		let totalLines = 0;

		const keys = getCollectionKeysFromCSV(obj.table);

		const model = createModel(obj.table, keys);

		fs.createReadStream(`./data/${obj.file}`)
			.pipe(es.split('\n'))
			.pipe(
				es
					.mapSync(line => {
						const lineArray = line.split('|');

						const dataObject = {};

						keys.forEach((field, i) => {
							dataObject[field] = lineArray[i];
						});

						model.insertMany(dataObject)
							.then(data => {
								if (obj.parent) {
									const updateKey = obj.table.toLowerCase();

									const pushQuery = {};
									pushQuery[updateKey] = data[0]._id;

									mongoose.model(obj.parent)
										.findOneAndUpdate(
											{ RowID: data[0].RowID },
											{ $push: pushQuery }
										)
										.then(res =>
											console.log(
												`${obj.parent}: ${res.RowID} successfully updated with ${obj.table}: ${data[0].RowID}`
											)
										)
										.catch(err => console.log(err));
								}
								console.log(
									`${obj.table}: ${data[0].RowID} successfully added`
								);
							})
							.catch(err => {
								console.log(err);
							});

						totalLines++;
						totalLines % 10000 === 0 ? console.log(totalLines) : null;
					})
					.on('end', () => {
						console.log(`${obj.file} Read`);
						console.log(totalLines);
					})
			)

			.on('error', err => console.log('File Read Error', err));
	});
};

insertData();
