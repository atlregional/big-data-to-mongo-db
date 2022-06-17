const { MongoClient } = require('mongodb');

const connectToDB = async ({dbName, dbConnectionURL}) => {
  const client = new MongoClient(dbConnectionURL);
  await client.connect();
  console.log(`\nConnected to ${dbName} database`);
  const db = client.db(dbName);
  return db;
}

connectToDB({
  dbName:  `ztraxTransaction04292022`,
  dbConnectionURL: 'mongodb://localhost:27017'
}).then(async db => {

  const searchArray = [
    'invitation',
    'ih borrower'
  ];

  const regexArray = [];

  for await (str of searchArray) {
    regexArray.push(new RegExp(str, 'i'))
  }

  const corporateBuyerStats = await db.collection('BuyerName').aggregate([
    {$lookup: 
      {
        from: 'Main',
        localField: 'TransId',
        foreignField: 'TransId',
        as: 'Transactions'
      }
    },
    {$lookup: 
      {
        from: 'PropertyInfo',
        localField: 'TransId',
        foreignField: 'TransId',
        as: 'Properties'
      }
    },
    {$addFields: 
      {
      Transaction: {
        $arrayElemAt: ["$Transactions", 0]
      },
      NumTransactions: {
        $size: "$Transactions"
      },
      NumProperties: {
        $size: "$Properties"
      }
      }
    },
    {$match: 
      {
        FIPS: '13121',
        "Transaction.AssessmentLandUseStndCode": /RR/i,
        // "Transaction.DocumentDate": 
        //   {
        //     $gte: new Date('2020-01-01'),
        //     $lte: new Date('2020-12-31'),
        //   },
        // BuyerNonIndividualName: /[a-zA-Z]/
        BuyerNonIndividualName: {$in: regexArray}

      }
    },
    {$group: {
      _id: "$BuyerNonIndividualName", 
      buyerNameCount: {$sum: 1},
      transactionCount: {$sum: "$NumTransactions"},
      propertyCount: {$sum: "$NumProperties"}

    }},
    // {$match: {buyerNameCount: {$gt: 10}}},
    {$sort: {propertyCount: -1}}
  ]).toArray();

  console.log(corporateBuyerStats);
  process.exit(0);
})