const data = require(`./extracts/${process.argv[2]}.json`);
const fs = require('fs');

// console.log(data);
const fileName = process.argv[3] || process.argv[2] || 'export' ;
const csvFilePath = `./csvs/${fileName}.csv`

const primaryFields = [

  // FOR TRANSACTIONS Main
    "RecordingDate",
    "DocumentDate",
    "EffectiveDate",
    "SalesPriceAmount",
    "SalesPriceAmountStndCode",
    "IntraFamilyTransferFlag",
    "PropertyUseStndCode",
    "AssessmentLandUseStndCode"
  
  // FOR TRANSACTIONS PropertyInfo
  // "AssessorParcelNumber",
  // "UnformattedAssessorParcelNumber",
  // "PropertyHouseNumber",
  // "PropertyHouseNumberExt",
  // "PropertyStreetPreDirectional",
  // "PropertyStreetName",
  // "PropertyStreetSuffix",
  // "PropertyStreetPostDirectional",
  // "PropertyBuildingNumber",
  // "PropertyFullStreetAddress",
  // "PropertyCity",
  // "PropertyState",
  // "PropertyZip",
  // "PropertyZip4",
  // "OriginalPropertyFullStreetAddress",
  // "OriginalPropertyAddressLastline",
  // "PropertyAddressStndCode",
  // "LegalLot",
  // "LegalOtherLot",
  // "LegalSubdivisionName",
  // "PropertySequenceNumber",
  // "PropertyAddressCensusTractAndBlock",
  // "FIPS",
  // "ImportParcelID",
  // "AssessmentRecordMatchFlag"

  // FOR ASSESSMENT
  // "RowID",
  // "ImportParcelID",
  // "AssessorParcelNumber",
  // "UnformattedAssessorParcelNumber",
  // "ParcelSequenceNumber",
  // "PropertyAddressLatitude",
  // "PropertyAddressLongitude",
  // "TaxYear"
];

const populatedFields = {
  // FOR TRANSACTIONS
  PropertyInfo : [
    "AssessorParcelNumber",
    "UnformattedAssessorParcelNumber",
    "PropertyHouseNumber",
    "PropertyHouseNumberExt",
    "PropertyStreetPreDirectional",
    "PropertyStreetName",
    "PropertyStreetSuffix",
    "PropertyStreetPostDirectional",
    "PropertyBuildingNumber",
    "PropertyFullStreetAddress",
    "PropertyCity",
    "PropertyState",
    "PropertyZip",
    "PropertyZip4",
    "OriginalPropertyFullStreetAddress",
    "OriginalPropertyAddressLastline",
    "PropertyAddressStndCode",
    "LegalLot",
    "LegalOtherLot",
    "LegalSubdivisionName",
    "PropertySequenceNumber",
    "PropertyAddressCensusTractAndBlock",
    "FIPS",
    "ImportParcelID",
    "AssessmentRecordMatchFlag"
  ]

  // "Main" : [
  //   "RecordingDate",
  //   "DocumentDate",
  //   "EffectiveDate",
  //   "SalesPriceAmount",
  //   "SalesPriceAmountStndCode",
  //   "IntraFamilyTransferFlag",
  //   "PropertyUseStndCode",
  //   "AssessmentLandUseStndCode"
  // ]

  // FOR ASSESSMENT
  // Building : [
  //   "PropertyCountyLandUseDescription",
  //   "PropertyCountyLandUseCode",
  //   "PropertyLandUseStndCode",
  //   "YearBuilt",
  //   "EffectiveYearBuilt",
  //   "YearRemodeled"  
  // ],
  // BuildingAreas : [
  //   "BuildingOrImprovementNumber",
  //   "BuildingAreaSequenceNumber",
  //   "BuildingAreaStndCode",
  //   "BuildingAreaSqFt",

  // ]
  // "SaleData" : [
  //   "SaleSeqNum",
  //   "SalesPriceAmount",
  //   "SalesPriceAmountStndCode",
  //   "RecordingDate"
  // ]
}

let headerStr = '';
primaryFields.forEach(field =>
  headerStr = headerStr + field + ','  
)
Object.entries(populatedFields).forEach(([key, array]) =>
  array.forEach(field =>
    headerStr = headerStr + key + '.' + field + ','
  )
);

const headerRow = headerStr.replace(/,$/, "\n")

fs.appendFile(csvFilePath, headerRow, err => err 
  ? console.log(err) 
  : data.forEach((obj, i) => {
    let str = '';
    primaryFields.forEach(field =>
      str = str + `"${obj[field].replace(/\0/g, '')}"` + ','  
    )
    Object.entries(populatedFields).forEach(([key, array]) =>
      array.forEach(field => {
        const substringFilteredArray = obj[key].filter(item => 
          key === 'BuildingAreas' 
            ? item['BuildingAreaStndCode'] === 'BAL' ||
              item['BuildingAreaSequenceNumber'] === '1'
            : true
        )
        const substring = substringFilteredArray.length > 0 
          ? substringFilteredArray[0][field].replace(/\0/g, '')
          : ''
    
        obj[key].length > 0
          ? str = str + `"${substring}"` + ','
          : str = str + ""
      })
    );
    
    const row = str.replace(/,$/, '\n')

    fs.appendFile(csvFilePath, row, err => 
      err
        ? (console.log(err), process.exit(1)) 
        : data.length === i + 1
          ? (console.log('Done!'), process.exit(0))
          : null
    );
  })
)