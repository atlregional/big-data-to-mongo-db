require('dotenv').config();
const fs = require('fs');
const es = require('event-stream');
const mongoose = require('mongoose');
var mongojs = require('mongojs');
const insertDocument = require('./utils/insertDocument');
const getCollectionInfo = require('./utils/getCollectionInfo');

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

// const getCollectionInfoFromCSV = tableName => {
// 	const arr = fs
// 		.readFileSync(`./manifest/${manifest}.csv`, 'utf-8')
// 		.split('\r\n')
// 		.filter(str => str.split(',')[0] === tableName);

// 	let keys = [];

// 	for (let i = 0; i < arr.length; i++) {
// 		keys.push({
// 			key: arr[i].split(',')[1],
// 			sorter: arr[i].split(',')[2]
// 		});
// 	}

// 	return keys.sort((a, b) => a.sorter - b.sorter).map(item => item.key);
// };

// const updateParent = async (obj, childId, rowId) => {
// 	const pushQuery = {};
// 	pushQuery[obj.table] = childId;

// 	await db.collection(obj.parent).update({ RowID: rowId }, { $push: pushQuery }, err =>
// 		err ? console.log(err) : null
// 	);
// };

// const insertDocument = async (line, collection, keys, obj) => {

//   const lineArray = line.split('|');
//   // console.log(lineArray);

//   const dataObject = {};

//   keys.forEach((field, i) => {
//     dataObject[field] = lineArray[i];
//   });

//   modelConfig.forEach(table => {
//     obj.table === table.parent ? (dataObject[table.table] = []) : null;
//   });

//   await collection.insert(
//     dataObject, 
//     async (err, data) => {
//       err ? console.log(err) : obj.parent 
//         ? await updateParent(obj, data._id, data.RowID)
//         : null;
//       // totalLines++
//     })
// };



const insertData = obj => new Promise((resolve, reject) => {
  let totalLines = 0;

	const [keys] = getCollectionInfo(obj.table, manifest);

  const stream = fs.createReadStream(`./data/ZAsmt/${obj.file}`, {highWaterMark: 1000});

	stream.pipe(es.split('\n'))
    .on('data', async line => {
      try {
        stream.pause();
        await insertDocument(db, line, keys, obj, modelConfig);
      } finally {
        totalLines++;
        console.log('line count: ', totalLines);
        stream.resume();
      }
    })
    .on('end', () => {
      console.log(`${obj.file} Processed with ${totalLines} Total Lines`,);
      // console.log(totalLines);
      stream.close()
      resolve();
    })
    .on('error', err => {
      console.log('File Read Error', err)
      reject();
    });
});

const configArr = modelConfig.map(obj => {
	return {
		table: obj.table,
		parent: obj.parent,
		file: `${obj.table}.txt`
	};
});

const run = async () => {
	// const log = obj => new Promise(resolve => setTimeout(() => {
  //   console.log(obj.table)
  //   resolve();
  // }, 2000))
  try {
		for await (const obj of configArr) {
			await insertData(obj);
      // await log(obj)
		}
    process.exit(0);
	} catch (err) {
		console.log(err);
		process.exit(1);
	}
};

run();
