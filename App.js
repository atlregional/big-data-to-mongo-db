require('dotenv').config();
const fs = require('fs');
const es = require('event-stream');
const mongoose = require('mongoose');
var mongojs = require('mongojs');

// const createModel = require('./utils/createModel');
const modelConfig = require('./utils/model-association-config.json');

// const databaseName = process.argv[3];
// const manifest = process.argv[2];

const databaseName = 'ztraxAssessment';
const manifest = 'ZTRAX-Assessment';

// ${process.env.MONGODB_URI}${databaseName}`

const MONGODB_URI = `mongodb://localhost/${databaseName}`;

var db = mongojs(MONGODB_URI);

// mongoose.connect(MONGODB_URI, {
// 	useNewUrlParser: true,
// 	useUnifiedTopology: true,
// 	useFindAndModify: false
// })
// .then(() => {
//   console.log('connected to database');
//   insertData({
// 		"table": "Main",
// 		"parent": null,
//     "file": "Main.txt"
// 	})
//   // run();
// })
// .catch(err => {
// 	console.log(err);
// 	process.exit(1);
// });

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

const updateParent = (obj, childId, rowId) => {
	const pushQuery = {};
	pushQuery[obj.table] = childId;

	db.collection(obj.parent).update({ RowID: rowId }, { $push: pushQuery }, err =>
		err ? console.log(err) : null
	);
};

const insertData = async obj => {
	let totalLines = 0;

	const keys = getCollectionKeysFromCSV(obj.table);

	const collection = db.collection(obj.table);

	fs.createReadStream(`./data/${obj.file}`)
		.pipe(es.split('\n'))
		.pipe(
			es
				.mapSync(line => {
					totalLines++;
					totalLines % 10000 === 0 ? console.log(totalLines) : null;

					const lineArray = line.split('|');

					const dataObject = {};

					keys.forEach((field, i) => {
						dataObject[field] = lineArray[i];
					});

					modelConfig.forEach(table => {
						obj.table === table.parent ? (dataObject[table.table] = []) : null;
					});

					collection.insert(dataObject, (err, data) => {
						err ? console.log(err) : null;

						obj.parent ? updateParent(obj, data._id, data.RowID) : null;
					});
				})
				.on('end', () => {
					console.log(`${obj.file} Read`);
					console.log(totalLines);
				})
		)
		.on('error', err => console.log('File Read Error', err));
};

const configArr = modelConfig.map(obj => {
	return {
		table: obj.table,
		parent: obj.parent,
		file: `${obj.table}.txt`
	};
});

const run = async () => {
	try {
		for (const obj of configArr) {
			await insertData(obj);
		}
	} catch (err) {
		console.log(err);
		process.exit(1);
	}
};

run();
