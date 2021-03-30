module.exports = (db, obj, childId, rowId) => new Promise((resolve, reject) => {
	const pushQuery = {};
	pushQuery[obj.table] = childId;

	db.collection(obj.parent).update({ RowID: rowId }, { $push: pushQuery }, err =>
		err ? reject(err) : resolve()
	);
});