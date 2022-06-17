const QueryBigData = require('./QueryBigData');
const queryGenerator = require('./utils/queryGenerator');
const { iterator } = require('./config.json');

const init = async () => {
  try 
     {
      // for await (const county of iterator.counties) {
      for (let i = 0; i < iterator.counties.length; i++) {
        await QueryBigData(queryGenerator(iterator.counties[i], 2018), true);
        console.log(`Query of ${iterator.counties[i]} for 2018 complete. `);
      }
      console.log('Iterated queries complete.')
      }
    catch (err) {
      console.log('Iterator Error: ',err);
      process.exit(1)
    }
};

init();


