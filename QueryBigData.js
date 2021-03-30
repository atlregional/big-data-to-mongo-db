require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');
const config = require('./config.json');
const createModel = require('./utils/createModel');
const getCollectionKeys = require('./utils/getCollectionKeys');

const useConfigJSON = process.argv[2] === 'configJSON' ? true : false
const queryConfig = useConfigJSON 
  ? require(`./query-configs/${process.argv[3]}.json`)
  : null

// const { AsyncParser, transforms: {flatten} } = require('json2csv');
// const fastCsv = require('fast-csv');
const databaseKey = queryConfig 
  ? queryConfig.databaseKey 
  : process.argv[2]

const {
  layoutPath, 
  database, 
  manifestPath, 
  joinField,
  postFilterCriteria
} = config[databaseKey];

const layout = require(layoutPath);
const queryCollection = !queryConfig ? process.argv[3] : queryConfig.collection;
const queryField = !queryConfig ? process.argv[4] : null;
const queryValue = !queryConfig ? process.argv[5] : null;

const queryObj = !queryConfig 
  ? {
      [queryField]: queryValue
    } 
  : queryConfig.queryObj;

const queryArray = queryConfig 
  ? queryConfig.queryArray
  : null



const outputFilename = !queryConfig ? process.argv[6] : `${process.argv[3]}.json`;

const MONGODB_URI = `mongodb://localhost/${database}`;

let lineCount = 0;

const buildModels = async () => {
  const models = {};
  try {
    for await (const obj of layout.filter(obj =>  !obj.exclude)) {
    await createModel(
        obj.table, 
        getCollectionKeys(obj.table, manifestPath, true),
        layout,
        joinField
      )
      .then(model =>
        models[obj.table] = model
      )
      .catch(err => console.log(err))
    }
  } finally {
    return models
  }
};

const postFilter = (obj, collection, field) => 
  postFilterCriteria && obj
    ? obj[collection].map(item =>
      postFilterCriteria.values.includes(item[field])
        ? true
        : false
      ).includes(true)
    : true;

const writeToFile = str =>  
  new Promise((resolve, reject) => {  
    fs.appendFile(
        `./extracts/${outputFilename}`, 
        `${str},`, 
        err => err
          ? reject(err) : 
          ( 
            lineCount++ ,
            lineCount % 500 === 0 
              ? console.log(`${lineCount} records processed.`)
              : null,
            resolve()
          )
      )
    }
  )


const runCursorQuery = (query, collection, populateCollections) => {
  const queryType = typeof query;
  console.log(queryType, query);
  console.log(populateCollections);

  const primaryQuery = collection.find(
    query,
    getCollectionKeys(queryCollection, manifestPath, true)
  )
  
  populateCollections.forEach(popCollection =>
    primaryQuery.populate(
      popCollection,
      getCollectionKeys(popCollection, manifestPath, true)

    )
  );

  const cursor = primaryQuery.cursor();
  
  cursor
    .on('data', async doc => {
      const obj = JSON.parse(JSON.stringify(doc));
      postFilterCriteria
        ? postFilter(obj, postFilterCriteria.collection, postFilterCriteria.field)
          ? await writeToFile(JSON.stringify(obj))
          : null
        : await writeToFile(JSON.stringify(obj))
      }
    )
    .on('end', () => 
      fs.appendFile(
        `./extracts/${outputFilename}`, 
        ']', 
        err => err 
          ? (console.log(err), process.exit(1)) 
          : (console.log('Done!'), process.exit(1))
      )
    )
    .on('error', err => {
      console.log(err);
      process.exit(1);
    }); 
};

const runQueryFromArray = (query, collection, populateCollections) => 
  new Promise ((resolve, reject) => {
    console.log(query);
    // console.log(populateCollections);

    const primaryQuery = collection.find(
      query,
      getCollectionKeys(queryCollection, manifestPath, true)
    );
    
    populateCollections.forEach(popCollection =>
      primaryQuery.populate(
        popCollection,
        getCollectionKeys(popCollection, manifestPath, true)

      )
    );

    primaryQuery.exec(async (err, doc) => {
      const obj = doc.length > 0 ? JSON.parse(JSON.stringify(doc[0])) : null;
      !err
        ? postFilterCriteria
          ? postFilter(obj, postFilterCriteria.collection, postFilterCriteria.field)
            ? (obj ? await writeToFile(JSON.stringify(obj)) : null, resolve())
            : resolve()
          : (obj ? await writeToFile(JSON.stringify(obj)) : null, resolve())
        : (consol.log(err), reject(err))
    });
  });

const arrayToQueryAsync = async (queryArray, collection, populateCollections) => {
    try {
      for await (const obj of queryArray) {
        // console.log(obj)
        await runQueryFromArray(obj, collection, populateCollections)
      }
      fs.appendFile(
        `./extracts/${outputFilename}`, 
        ']', 
        err => err 
          ? (console.log(err), process.exit(1)) 
          : (console.log('Done!'), process.exit(1))
      )
    }
    catch (err) {
        console.log(err);
        process.exit(1);
    }
  }

mongoose.connect(MONGODB_URI, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
	useFindAndModify: false
})
.then(async () => {
  const db = await buildModels();
  console.log(db);
  const collection = db[queryCollection];
  // console.log('models', db[queryCollection]);
  console.log('Connected to:', database);
  console.log('Collection:', queryCollection);
  console.log('Query Params', queryObj);

  const populateCollections = layout.filter(obj =>
    obj.table !== queryCollection &&
    !obj.exclude  
  ).map(obj => obj.table);

  console.log('Populated with:', populateCollections);

  fs.appendFile(
    `./extracts/${outputFilename}`, 
    '[', 
    err => err ? console.log(err) : null
  );

  queryArray
   ? arrayToQueryAsync(queryArray, collection, populateCollections)
   : runCursorQuery(queryObj, collection, populateCollections)

})
.catch(err => {
  console.log(err);
  process.exit(1);
});







