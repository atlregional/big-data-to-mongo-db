const fs = require('fs');

module.exports = (tableName, manifest) => {
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