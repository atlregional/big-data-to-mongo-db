// const updateParent = require('./updateParent');

module.exports = (db, data, collectionName) => new Promise((resolve, reject) => {

  const collection = db.collection(collectionName);

  // const lineArray = line.split('|');
  // console.log(lineArray);

  // const dataObject = {...data};

  collection.insert(
    data, 
    (err, data) => {
      err 
        ? reject(err) 
          // : obj.parent 
          // ? updateParent(db, obj, data._id, data.RowID)
          //     .then(() => resolve())
          //     .catch(err => reject(err))
        : resolve();
      // totalLines++
    })
});