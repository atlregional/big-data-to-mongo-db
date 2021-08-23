require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');
const dbConfig = require('./config.json');
const createModel = require('./utils/createModel');
const getCollectionKeys = require('./utils/getCollectionKeys');

const useConfigJSON = process.argv[2] === '-useConfig';
const queryConfig = useConfigJSON 
  ? require(`./query-configs/${process.argv[3]}`)
  : null

const databaseKey = queryConfig 
  ? queryConfig.databaseKey 
  : process.argv[2];

const relatedDatabaseKey = queryConfig 
? queryConfig.relatedDatabaseKey 
: null;

const {
  layoutPath, 
  database, 
  manifestPath, 
  // joinField,
  // postFilterCriteria
} = dbConfig[databaseKey];

// const database2 = dbConfig[relatedDatabaseKey].database

// const {
//   layoutPath2, 
//   database2, 
//   manifestPath2, 
//   joinField2,
//   // postFilterCriteria
// } = dbConfig[relatedDatabaseKey];

const { postFilterCriteria } = queryConfig

const layout = require(layoutPath);
// const layout2 = require(dbConfig[relatedDatabaseKey].layout);


const queryCollection = !queryConfig ? process.argv[3].split('.')[0] : queryConfig.collection;
const queryField = !queryConfig ? process.argv[4] : null;
const queryValue = !queryConfig ? process.argv[5] : null;

const queryObj = !queryConfig 
  ? {
      [queryField]: queryValue
    } 
  : queryConfig.queryObj;

// queryConfig.queryObj
//   ? Object.entries(queryConfig.queryObj).forEach(([key,value]) => {
//       const type = typeof value;
//       const pmatch = type !== 'string'
//         ? Array.isArray(value.pmatch)
//         ? value.pmatch.map(item => `/${item}/`)
//           : `/${value.pmatch}/`
//         : null
//       const matchObj = type !== 'string'
//         ? ({
//             [value.op] : pmatch 
//           })
//         : {};
//       type == 'string'
//         ? queryObj[key] = value
//         : queryObj[key] = new RegExp(matchObj)
//       // queryObj[property] = 
//   })
//   : null

const queryArray = queryConfig 
  ? queryConfig.queryArray
  : null;

const buffered = queryArray 
  ? queryArray.length > 200 
    ? true 
    : false
  : false  
const bufferSize = queryArray 
  ? queryArray.length < 500 
    ? queryArray.length 
    : 500
  : null;

const bufferedArray = [];
let buffer = [];

queryArray
  ? queryArray.forEach((obj, i) => 
      i < bufferSize * (bufferedArray.length + 1) && i !== queryArray.length - 1
        ? buffer.push(obj.ImportParcelID.toString())
        : (bufferedArray.push(buffer), buffer = [obj.ImportParcelID.toString()])
    )
  : null

const outputFilename = !queryConfig 
  ? process.argv[6]
    : queryConfig.outputFilename
      ? `${queryConfig.outputFilename}.json`
      : `${process.argv[3].split('.')[0]}-extract.json`;

const MONGODB_URI = `mongodb://localhost/${database}`;
// const relateMONGODB_URI = `mongodb://localhost/${database2}`;



let lineCount = 0;

const buildModels = databaseKey => {
  const modelConfig = dbConfig[databaseKey]
  const models = {};
  require(modelConfig.layoutPath).filter(obj =>  obj.include)
    .forEach(obj =>
      models[obj.table] = createModel(
        obj.table, 
        getCollectionKeys(obj.table, modelConfig.manifestPath, true),
        require(modelConfig.layoutPath),
        modelConfig.joinField
      )
    )
  return models;
};

const db = buildModels(databaseKey);
// const db2 = buildModels(relatedDatabaseKey);





// console.log(db);
const collection = db[queryCollection];
// const outputCollection = db[outputCollectionName];




// console.log('Building models start')
//   try {
//     for await (const obj of layout.filter(obj =>  !obj.exclude)) {
//     console.log(obj); 
//     await createModel(
//         obj.table, 
//         await getCollectionKeys(obj.table, manifestPath, true),
//         layout,
//         joinField
//       )
//       .then(model =>
//         models[obj.table] = model
//       )
//       .catch(err => console.log(err))
//     }
//   } finally {
//     return models
//   }
// };

// const insertManyToOutputDB = async obj => {
//   outputCollection.insertMany(obj)
// }

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
        `${str},\n`, 
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

const runCursorQuery = (query, collection, populateCollections) =>
  new Promise((resolve, reject) => {
  const isArray = Array.isArray(query);
  const arrayQuery = isArray ? {ImportParcelID : { $in : query}} : null;
  const parsedQuery = {};
  Object.keys(query)[0]
    ? Object.entries(query).forEach(([key,value]) =>
      parsedQuery[key] = !value.type
        ? value
        : value.type === 'in'
          ? { $in: [...value.values.map(item => new RegExp(item)
            )]}
          : value.type === 'regex'
            ? { $regex: new RegExp(value.values) }
            : null
    )
    : null;

  // console.log(parsedQuery);

  const primaryQuery = collection.find(
    arrayQuery 
      ? arrayQuery 
      : parsedQuery,
    getCollectionKeys(queryCollection, manifestPath, true)
  )
  
  populateCollections.forEach(popCollection =>
    primaryQuery.populate({
      path: popCollection,
      select: getCollectionKeys(popCollection, manifestPath, true)

    })
  );

  const cursor = primaryQuery.cursor({batchSize : 500});
  
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
       !buffered 
        ? fs.appendFile(
            `./extracts/${outputFilename}`, 
            ']', 
            err => err 
              ? (console.log(err), process.exit(1)) 
              : (console.log('Done!'), process.exit(1))
          )
        : resolve()
    )
    .on('error', err => {
      !buffered 
        ? (console.log(err), process.exit(1))
        : reject(err)
    }); 
});

const runQueryFromArray = (query, collection, populateCollections) => 
  new Promise ((resolve, reject) => {

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
      obj ? console.log('\u2705') : console.log('\u0274')
      !err
        ? postFilterCriteria
          ? postFilter(obj, postFilterCriteria.collection, postFilterCriteria.field)
            ? (obj ? await writeToFile(JSON.stringify(obj)) : null, resolve())
            : resolve()
          : (obj ? await writeToFile(JSON.stringify(obj)) : null, resolve())
        : (consol.log(err), reject(err))
    });
  });

const arrayToQueryAsync = async (query, collection, populateCollections) => {
    try {
      for await (const obj of query) {
        const isArray = Array.isArray(obj)
        // console.log(obj)
        isArray 
          ? await runCursorQuery(obj, collection, populateCollections)
          : await runQueryFromArray(obj, collection, populateCollections)
      };
      fs.appendFile(
        `./extracts/${outputFilename}`, 
        ']', 
        err => err 
          ? (console.log(err), process.exit(1)) 
          : (console.log(`Done! ${lineCount} Records Found.`), process.exit(0))
      )
    }
    catch (err) {
      console.log(err);
      process.exit(1);
    }
    // finally {
    //   fs.appendFile(
    //     `./extracts/${outputFilename}`, 
    //     ']', 
    //     err => err 
    //       ? (console.log(err), process.exit(1)) 
    //       : (console.log(`Done! ${lineCount} Records Found.`), process.exit(0))
    //   )
    // }

  }


  mongoose.connect(MONGODB_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useFindAndModify: false
  })
  .then(async () => {

    console.log('Primary database', database, 'connected');
    console.log('Collection:', queryCollection);
    console.log('Query Params:', queryObj ? queryObj : 'via Array');
  
    buffered 
      ? console.log(`There are ${queryArray.length} records (${bufferedArray.length} batches) to query.`)
      : null 
  
    const populateCollections = layout.filter(obj =>
      obj.table !== queryCollection &&
      obj.include  
    ).map(obj => obj.table);
  
    console.log('Populated with:', populateCollections);
  
    fs.appendFile(
      `./extracts/${outputFilename}`, 
      '[', 
      err => err ? console.log(err) : null
    );
  
    queryArray
     ? arrayToQueryAsync(buffered ? bufferedArray : queryArray, collection, populateCollections)
     : runCursorQuery(queryObj, collection, populateCollections)
  
  })
  .catch(err => {
    console.log(err);
    process.exit(1);
  });









