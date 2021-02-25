const fs = require('fs');
const es = require('event-stream');
var mongojs = require('mongojs');

const databaseName = 'ztraxhistoric';
const collectionName = 'main';

var databaseUrl = `mongodb://localhost/${databaseName}`;
var db = mongojs(databaseUrl);

// Set the Mongo DB collection into which you are inserting your data
const myCollection = db.collection(collectionName);

// Get array of filenames from directory using fs.readdir
const fileName = 'ZAsmt-Main.txt'; 

const filePath = `../data/ztrax/historic/GA/${fileName}`;

var totalLines = 0;


// Pull header array from manifest CSV 
const headerArray;


// Loop over filenames and console.log(dataObject) for each row in each file
fs.createReadStream(filePath)
  .pipe(es.split('\r\n'))
  .pipe(es.mapSync(line => {
    const lineArray = line.split("|");

    const dataObject = {};

    headerArray.forEach((field, i) => 
        dataObject[field] = lineArray[i]);
        
    

    // myCollection.insert(dataObject) 

    totalLines++;
    totalLines % 10000 === 0 ? console.log(totalLines) : null;
  })
  .on('end', () => {
    console.log('Entire File Read');
    console.log(totalLines);
  }))
  .on('error', err => console.log('File Read Error', err)
  )