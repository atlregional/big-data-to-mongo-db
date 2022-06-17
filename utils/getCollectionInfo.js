const fs = require('fs');

module.exports = (tableName, manifestPath, query) => {
	const arr = fs
		.readFileSync(manifestPath, 'utf-8')
		.split('\r\n')
		.filter(str => str.split(',')[0] === tableName)
    .filter(str => query ? str.split(',')[6] === 'TRUE' : true);

	let fieldInfo = [];

	for (let i = 0; i < arr.length; i++) {
		fieldInfo.push({
			key: arr[i].split(',')[1],
			sorter: arr[i].split(',')[2],
      type: arr[i].split(',')[3]
		});
	};

  const fieldTypes = {};

  fieldInfo.forEach(item => fieldTypes[item.key] = item.type)

	return [
    fieldInfo.sort((a, b) => a.sorter - b.sorter).map(item => item.key),
    fieldTypes
  ];
};