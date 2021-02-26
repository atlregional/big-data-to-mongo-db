const fs = require('fs');
const es = require('event-stream');
var mongojs = require('mongojs');

const databaseName = 'ztraxhistoric';
// const collectionName = 'main';

var databaseUrl = `mongodb://localhost/${databaseName}`;
var db = mongojs(databaseUrl);

db.on('error', err => {
	console.log(err);
	process.exit(1);
});

// Set the Mongo DB collection into which you are inserting your data
// const myCollection = db.collection(collectionName);

// Get array of filenames from directory using fs.readdir
// const fileName = 'ZAsmt-Main.txt';

// const filePath = `../data/ztrax/historic/GA/${fileName}`;

const getCollectionKeysFromCSV = tableName => {
	const arr = fs.readFileSync('./manifest/ZTRAX-Assessment.csv', 'utf-8').split('\r\n');

	let keys = [];

	for (let i = 1; i < arr.length; i++) {
		arr[i].split(',')[0] === tableName ? keys.push(arr[i].split(',')[1]) : null;
	}

	return keys;
};

const getCollectionNamesFromCSV = () => {
	const arr = fs.readFileSync('./manifest/ZTRAX-Assessment.csv', 'utf-8').split('\r\n');

	let collections = [];

	for (let i = 1; i < arr.length; i++) {
		collections.push(arr[i].split(',')[0]);
	}

	return [...new Set(collections)];
};

const insertData = () => {
	const files = fs.readdirSync('./data', 'utf-8');
	const collections = getCollectionNamesFromCSV();

	files.forEach(file => {
		const fileName = file.split('.')[0];
		let totalLines = 0;

		if (!collections.includes(fileName)) {
			console.log(`Error: ${fileName}`);
			continue;
		}

		const keys = getCollectionKeysFromCSV(fileName);

		fs.createReadStream(`./data/${file}`)
			.pipe(es.split('\n'))
			.pipe(
				es
					.mapSync(line => {
						const lineArray = line.split('|');
						const dataObject = {};
						keys.forEach((field, i) => {
							dataObject[field] = lineArray[i];
						});

						// console.log(dataObject);

						db.collection(fileName).insert(dataObject);

						totalLines++;
						totalLines % 10000 === 0
							? console.log(totalLines)
							: null;
					})
					.on('end', () => {
						console.log(`${file} Read`);
						console.log(totalLines);
					})
			)

			.on('error', err => console.log('File Read Error', err));
	});
};

insertData();
