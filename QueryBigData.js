require('dotenv').config();
const mongoose = require('mongoose');
const fs = require('fs');
const createModel = require('./utils/createModel');
const getCollectionKeys = require('./utils/getCollectionKeys');
const relationalLayout = require('./layout/ZAsmt-Layout.json');

const queryDatabase = process.argv[2];
const queryCollection = process.argv[3];
const queryField = process.argv[4];
const queryValue = process.argv[5];
const queryObj = {
  [queryField]: queryValue
};

const MONGODB_URI = `mongodb://localhost/${queryDatabase}`;


const buildModels = async () => {
  const models = {};

  // relationalLayout.forEach(obj => 
  try {
    for await (const obj of relationalLayout) {
    await createModel(obj.table, getCollectionKeys(obj.table, 'ZTRAX-Assessment'))
      .then(model =>
        models[obj.table] = model
      )
      .catch(err => console.log(err))
    }
  } finally {
    return models
  }
};

mongoose.connect(MONGODB_URI, {
	useNewUrlParser: true,
	useUnifiedTopology: true,
	useFindAndModify: false
})
.then(async () => {
  const db = await buildModels()
  const collection = db[queryCollection];
  // console.log('models', db[queryCollection]);
  console.log('Connected to:', queryDatabase);
  console.log('Querying:', queryCollection);
  console.log('Query Params', queryObj);

  // consy
  // console.log(collection)
  // connection.getCollection(queryCollection).find(queryObj).then(res => console.log(res));

  // db.Main.find({ "RowID" : queryValue}).then(res => console.log(res))
  // const writeStream = fs.createWriteStream('text.json')
  collection
    .find(queryObj)
    .populate('Building')
    .exec((err, res) => {
      // const array = [];
      // res.forEach(item => {
      //   const obj = {...item};
      //   obj['Building'] = item.Building;
      //   array.push(obj);
      // })
      // console.log(array)
      fs.writeFile('test.json', JSON.stringify(res), err => err ? console.log(err) : console.log('Done!'))
    })
    // .then(res => console.log(res))
    // .catch(err => console.log(err));

})
.catch(err => {
	console.log(err);
	process.exit(1);
});






