require('dotenv').config();
const fs = require('fs');
const es = require('event-stream');
const mongoose = require('mongoose');
var mongojs = require('mongojs');


const createModel = require('./utils/createModel');
const modelConfig = require('./utils/model-association-config.json');
// const { config } = require('process');

// const databaseName = process.argv[3];
// const manifest = process.argv[2];

const databaseName = 'ztraxAssessment';
const manifest = 'ZTRAX-Assessment';

const MONGODB_URI = `${process.env.MONGODB_URI}${databaseName}` || `mongodb://localhost/${databaseName}`;



var db = mongojs(MONGODB_URI);

// Set the Mongo DB collection into which you are inserting your data
const myCollection = db.collection('Main');


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

const insertData = async obj => {
	// Adding file property to objs in config array
  let totalLines = 0;

  const keys = getCollectionKeysFromCSV(obj.table);

  // const model = await createModel(obj.table, keys);
  // console.log(obj, model.schema);

  fs.createReadStream(`./data/ZAsmt/${obj.file}`)
    .pipe(es.split('\n'))
    .pipe(es.mapSync(line => {
        const lineArray = line.split('|');

        const dataObject = {};

        keys.forEach((field, i) => {
          dataObject[field] = lineArray[i];
        });
        
        modelConfig.forEach(table => {
          obj.table === table.parent ? dataObject[table.table] = []
          : null
        })


        // console.log(model);


        // myCollection.insert(dataObject);

        // console.log(dataObject);

        // model
        
        myCollection.insert(dataObject)
          // .then(data => {

          //   totalLines++;
          //   totalLines % 10000 === 0 ? console.log(totalLines) : null;
    
          //   // if (obj.parent) {
          //   //   const updateKey = obj.table.toLowerCase();

          //   //   const pushQuery = {};
          //   //   pushQuery[updateKey] = data[0]._id;

          //   //   mongoose.model(obj.parent)
          //   //     .findOneAndUpdate(
          //   //       { RowID: data[0].RowID },
          //   //       { $push: pushQuery }
          //   //     )
          //   //     // .then(res =>
          //   //     // 	console.log(
          //   //     // 		`${obj.parent}: ${res.RowID} successfully updated with ${obj.table}: ${data[0].RowID}`
          //   //     // 	)
          //   //     // )
          //   //     .catch(err => console.log(err));
          //   // }
          //   // console.log(
          //   // 	`${obj.table}: ${data[0].RowID} successfully added`
          //   // );
          // })
          // .catch(err => {
          //   console.log(err);
          // });
      })
      .on('end', () => {
        console.log(`${obj.file} Read`);
        console.log(totalLines);
      })
      // .catch(err => console.log(err))
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

  insertData({
		"table": "Main",
		"parent": null,
    "file": "Main.txt"
	})



// const run = async () => {
//   for (const obj of configArr) {
//       // await console.log(obj);
//     await insertData(obj)
//   }
// }

// run();
// configArr.forEach(obj => insertData(obj));


// insertData();
