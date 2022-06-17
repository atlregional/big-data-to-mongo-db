const fs = require('fs');

module.exports = (str, filePath, simple) =>  
    new Promise((resolve, reject) => {  
      fs.appendFile(
          filePath,
          simple 
          ? `${str}\n` 
          : `${JSON.stringify(str)},\n`, 
          err => err
            ? reject(err) : 
              resolve()
        )
      }
    )