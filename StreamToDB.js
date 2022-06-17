require('dotenv').config();
const fs = require('fs');
const csv = require('csv-stream')
const through2 = require('through2')
const mongojs = require('mongojs');
const moment = require('moment');


const insertDocument = require('./utils/insertDocument');
const getCollectionInfo = require('./utils/getCollectionInfo');
const valueFormatter = require('./utils/valueFormatter');
// const getTableNamesFromCSV = require('./utils/getTableNamesFromCSV');
// const modelConfig = require('./utils/model-association-config.json');

const dataDir = process.argv[2]
const manifest = process.argv[3];
const databaseName = process.argv[4];

const MONGODB_URI = `mongodb://localhost/${databaseName}`;

const db = mongojs(MONGODB_URI);

// const configArr = modelConfig.map(obj => {
// 	return {
// 		table: obj.table,
// 		parent: obj.parent,
// 		file: `${obj.table}.txt`
// 	};
// });

const fileNames = [];

fs.readdirSync(`./data/${dataDir}/`)
  .filter(file => file[0] !== '.')
  .forEach(file => 
    fileNames.push(file)
  );

// console.log(fileNames);

const insertDataPipeline = fileName => new Promise((resolve, reject) => {
  let totalLines = 0;

  const collectionName = fileName.replace('.txt', '');

	const [fields, fieldTypes] = getCollectionInfo(collectionName, manifest);
  // console.log(fieldTypes);
  
  console.log('Started', collectionName);

  const stream = fs.createReadStream(`./data/${dataDir}/${fileName}`);
  stream
    .pipe(csv.createStream({
      delimiter: '|',
      endLine: '\n',
      columns: fields
    }))
    .pipe(through2({ objectMode: true }, (data, enc, cb) => {
      const hasLocationData = Object.keys(data).filter(key =>  
        key.includes('Latitude') || key.includes('Longitude')    
      )[0];

      const formattedData = {};

      Object.entries(data).forEach(([key,value]) =>
        formattedData[key] = valueFormatter(value, fieldTypes[key] )
      )

      // ADD GEOMETRY IF THERE'S LAT AND LONG
      if (hasLocationData) {
        formattedData.Geometry = {
          type: 'Point',
          coordinates: [
            data['PropertyAddressLongitude'] ? Number(data['PropertyAddressLongitude']) : 0,
            data['PropertyAddressLatitude'] ? Number(data['PropertyAddressLatitude']) : 0
          ]
        }
      };

      insertDocument(db, formattedData, collectionName)
        .then(() => {
          cb(null, true);    
        })
        .catch(err => {
          cb(err, null);
        })
    }))
    .on('data', data => {
      totalLines++          
      totalLines % 100000 === 0 
        ? console.log('Line', totalLines, `of ${fileName} processed at ${moment().format("dddd, MMMM Do YYYY, h:mm:ss a")}`)
        : null
    })
    .on('end', () => {
      resolve();
    })
    .on('error', err => {
      reject(err);
    })

});

const run = async () => {
  try {
		for await (const fileName of fileNames) {
      await insertDataPipeline(fileName);
      console.log(fileName, 'added successfully');
		};
    console.log(`Completed at ${moment().format("dddd, MMMM Do YYYY, h:mm:ss a")}`);
    process.exit(0);
	} 
  catch (err) {
		console.error('Pipeline failed', err);
		process.exit(1);
	}
};

run();
