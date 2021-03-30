const fs = require('fs');

const getCollectionNamesFromCSV = () => {
	const arr = fs.readFileSync('./manifest/ZTRAX-Assessment.csv', 'utf-8').split('\r\n');

	let collections = [];

	for (let i = 1; i < arr.length; i++) {
		collections.push(arr[i].split(',')[0]);
	}

	return [...new Set(collections)];
};

module.exports = getCollectionNamesFromCSV;
