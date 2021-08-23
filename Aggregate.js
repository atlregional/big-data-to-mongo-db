const data = require('./extracts/DecaturAssessments.json');

// console.log(data.filter(obj => obj.Building.length === 0).length);

const aggregate = {
  by: {
    collection : 'Building',
    field : 'PropertyLandUseStndCode'
    // field: 'DocumentDate',
    // parser : (obj, collection, field) => obj[collection][0][field].slice(0,4)
  },
  type : 'count'
}

const aggregation = {};

data.forEach(obj => {
  const aggregator = 
    aggregate.by.collection
      ? aggregate.by.parser
        ? aggregate.by.parser(obj, aggregate.by.collection, aggregate.by.field)
        : obj[aggregate.by.collection][0][aggregate.by.field]
    : obj[aggregate.by.field]

  switch (aggregate.type) {
    case 'count' : {
      aggregation[aggregator]
        ? aggregation[aggregator] = aggregation[aggregator] + 1
        : aggregation[aggregator] = 1
    }
  }
})

console.log(aggregation)