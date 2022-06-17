const config = require('../config');
module.exports = async (headerArray, obj, county) => {
  const array = [];
  const {  Transactions: transactions } = obj;
  const transactionsWithSalePrice = transactions.filter(transaction =>
    transaction['SalesPriceAmount'] !== '' 
  );

  headerArray.forEach(header => {
    switch (header) {
      case 'ImportParcelID':
      case 'PropertyAddressLatitude':
      case 'PropertyAddressLongitude':
      case 'PropertyFullStreetAddress':
      case 'PropertyCity':
      case 'PropertyZip':
        array.push(JSON.stringify(obj[header]));
        break;
      case 'PropertyLandUseStndCode':
      case 'PropertyCountyLandUseDescription':
      case 'YearBuilt':
      case 'EffectiveYearBuilt':
      case 'YearRemodeled':
        array.push(obj['Building'][0] 
        ? JSON.stringify(obj['Building'][0][header])
        : null)
        break;
      case 'TotalTransactions2013':
      case 'TotalTransactions2014': 
      case 'TotalTransactions2015': 
      case 'TotalTransactions2016': 
      case 'TotalTransactions2017': 
      case 'TotalTransactions2018': 
      case 'TotalTransactions2019': 
      case 'TotalTransactions2020': 
        array.push(
          transactions.filter(transaction =>
            transaction['DocumentDate']
              .search(
                header.replace('TotalTransactions','')
              ) !== -1
          ).length
        )
        break;
      case 'TotalTransactionsWithSalePrice2013':
      case 'TotalTransactionsWithSalePrice2014': 
      case 'TotalTransactionsWithSalePrice2015': 
      case 'TotalTransactionsWithSalePrice2016': 
      case 'TotalTransactionsWithSalePrice2017': 
      case 'TotalTransactionsWithSalePrice2018': 
      case 'TotalTransactionsWithSalePrice2019': 
      case 'TotalTransactionsWithSalePrice2020': 
        array.push(
          transactionsWithSalePrice.filter(transaction =>
            transaction['DocumentDate']
              .search(
                header.replace('TotalTransactionsWithSalePrice','')
              ) !== -1
          ).length
        )
        break;
      case 'MaxSalePrice2013':
      case 'MaxSalePrice2014': 
      case 'MaxSalePrice2015': 
      case 'MaxSalePrice2016': 
      case 'MaxSalePrice2017': 
      case 'MaxSalePrice2018': 
      case 'MaxSalePrice2019': 
      case 'MaxSalePrice2020':
        const salePrices = transactionsWithSalePrice
        .filter(transaction =>
          transaction['DocumentDate']
            .search(
              header.replace('MaxSalePrice','')
            ) !== -1
        )
        .map(transaction =>
          parseInt(transaction['SalesPriceAmount'])
        );
        const maxSalePrice = salePrices[0] ? Math.max(...salePrices) : null;
        array.push(maxSalePrice)
        break;
      case 'BAB_BaseBuildingArea':
      case 'BAE_EffectiveBuildingArea':
      case 'BAF_FinishedBuildingArea':
      case 'BAG_GrossBuildingArea':
      case 'BAH_HeatedBuildingArea':
      case 'BAJ_AdjustedBuildingArea':
      case 'BAL_LivingBuildingArea':
      case 'BAP_PerimeterBuildingArea':
      case 'BAQ_Manufacturing':
      case 'BAT_TotalBuildingArea':
      case 'BLF_LivingBuildingAreaFinished':
      case 'BLU_LivingBuildingAreaUnfinished':
        const code = header.substring(0,3);
        const buildingAreaArrayFiltered = obj['BuildingAreas'].filter(buildingarea =>
          buildingarea['BuildingAreaStndCode'] === code
        );
        array.push(buildingAreaArrayFiltered[0]
          ? buildingAreaArrayFiltered[0]['BuildingAreaSqFt']
          : null
        );
        break;

      case 'BuildingAreaInferred':
        const buildingAreaArrayFilteredByConfig = obj['BuildingAreas'].filter(buildingarea =>
          buildingarea['BuildingAreaStndCode'] === config.buildingAreaStndCodeCountyPref[county]
        );
        array.push(buildingAreaArrayFilteredByConfig[0]
          ? buildingAreaArrayFilteredByConfig[0]['BuildingAreaSqFt']
          : null
        );
        break;

      case 'BuildingAreaInferredCode':
        array.push(JSON.stringify(
          config.buildingAreaStndCodeCountyPref[county])
        );
        break;
      default:
        break;
    } 
  });
  return array.join(',')
}