const writeToFileAsync = require('./writeToFileAsync')
const findRecordInJSONByUniqueValues = require('./findRecordInJSONByUniqueValue.js');
const aggregateTransactionsToCSVRow = require('./aggregateTransactionsToCSVRow'); 
const county = process.argv[2];


const joinOneToMany = async (obj) => {
  console.log(`Beginning data join for ${county} County...`);

  let lineCount = 0;
  const {onePath, manyPaths, pKey, fNestedKey, destPath, headerArray} = obj;

  const logFreq = 500;

  const firstLine = headerArray ? headerArray.join(',') : '[';
  const lastLine = ']';

  const ones = require(onePath);
  await writeToFileAsync(firstLine, destPath, true);
  for await (const one of ones) {
    const obj = one;
    one['Transactions'] = [];
    for await (const manyPath of manyPaths) {
      const many = require(manyPath).filter(otherObj =>
        otherObj[fNestedKey[0]][0][fNestedKey[1]] === obj[pKey]
      )
      many.forEach(each =>
        one['Transactions'].push(each)
      );
    }

    const csvRow = await aggregateTransactionsToCSVRow(headerArray, one, county);

    await writeToFileAsync(headerArray ? csvRow : one, destPath, headerArray).then(() => {
      lineCount++;
      if (lineCount % logFreq === 0) {
        console.log(`${lineCount} records processed.`)
      }
    });
  }
  await writeToFileAsync(lastLine, destPath, true);

}

const iterateArray = [
    2020,
    2019,
    2018,
    2017,
    2016,
    2015,
    2014,
    2013
]
const pathArray = iterateArray.map(str =>
  `../extracts/transactions/${str}/${county}-Transactions${str}.json`
);

joinOneToMany({
  onePath : `../extracts/assessments/${county}-Parcels.json`, 
  manyPaths : pathArray, 
  pKey : 'ImportParcelID', 
  fNestedKey: ['PropertyInfo', 'ImportParcelID'], 
  destPath : `../csvs/parcels-with-transactions-and-building-area/${county}-Parcels-with-Transactions-and-Building-Area.csv`,
  headerArray: [
    'ImportParcelID',
    'PropertyAddressLatitude',
    'PropertyAddressLongitude',
    'PropertyFullStreetAddress',
    'PropertyCity',
    'PropertyZip',
    'PropertyLandUseStndCode',
    'PropertyCountyLandUseDescription',
    'YearBuilt',
    'EffectiveYearBuilt',
    'YearRemodeled',
    'BuildingAreaInferred',
    'BuildingAreaInferredCode',
    // 'BAB_BaseBuildingArea',
    // 'BAE_EffectiveBuildingArea',
    // 'BAF_FinishedBuildingArea',
    // 'BAG_GrossBuildingArea',
    // 'BAH_HeatedBuildingArea',
    // 'BAJ_AdjustedBuildingArea',
    // 'BAL_LivingBuildingArea',
    // 'BAP_PerimeterBuildingArea',
    // 'BAQ_Manufacturing',
    // 'BAT_TotalBuildingArea',
    // 'BLF_LivingBuildingAreaFinished',
    // 'BLU_LivingBuildingAreaUnfinished',
    'TotalTransactions2013',
    'TotalTransactions2014',
    'TotalTransactions2015',
    'TotalTransactions2016',
    'TotalTransactions2017',
    'TotalTransactions2018',
    'TotalTransactions2019',
    'TotalTransactions2020',
    'TotalTransactionsWithSalePrice2013',
    'TotalTransactionsWithSalePrice2014',
    'TotalTransactionsWithSalePrice2015',
    'TotalTransactionsWithSalePrice2016',
    'TotalTransactionsWithSalePrice2017',
    'TotalTransactionsWithSalePrice2018',
    'TotalTransactionsWithSalePrice2019',
    'TotalTransactionsWithSalePrice2020',
    'MaxSalePrice2013',
    'MaxSalePrice2014',
    'MaxSalePrice2015',
    'MaxSalePrice2016',
    'MaxSalePrice2017',
    'MaxSalePrice2018',
    'MaxSalePrice2019',
    'MaxSalePrice2020'
  ]
})
.then(() => console.log('Done!'))
.catch(err => console.log(err))