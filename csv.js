const csvParser = require('csv-parser');
const fs = require('fs');
const path = require('path');
const fastCsv = require('fast-csv');

const dataDir = path.join(__dirname, 'data');
const outputFile = path.join(__dirname, 'aggregated_data.csv');
const expectedHeaders = [
  'open_time',
  'open',
  'high',
  'low',
  'close',
  'volume',
  'close_time',
  'quote_volume',
  'count',
  'taker_buy_volume',
  'taker_buy_quote_volume',
  'ignore',
];

async function readCsvFiles(directory) {
  const files = fs
    .readdirSync(directory)
    .filter((file) => file.endsWith('.csv'));
  const allData = [];

  for (const file of files) {
    const filePath = path.join(directory, file);
    const data = await new Promise((resolve, reject) => {
      const rows = [];
      fs.createReadStream(filePath)
        .pipe(
          csvParser({
            headers: expectedHeaders,
            skipLines: 0,
            mapValues: ({ header, index, value }) => {
              if (header === 'open_time') {
                return Number(value); // Ensure open_time is numeric
              }
              return value;
            },
          })
        )
        .on('data', (row) => rows.push(row))
        .on('end', () => resolve(rows))
        .on('error', reject);
    });
    allData.push(...data);
  }

  // Sort data by open_time
  allData.sort((a, b) => a.open_time - b.open_time);

  return allData;
}

function writeCsvFile(data, filePath) {
  fastCsv
    .write(data, { headers: expectedHeaders })
    .pipe(fs.createWriteStream(filePath))
    .on('finish', () => console.log(`Combined CSV written to ${filePath}`));
}

async function combineAndSortCsvFiles() {
  const combinedData = await readCsvFiles(dataDir);

  const data = {
    symbol: 'IMXUSDT',
    timeframe: '15m',
    dateEnd: '2022-01-01 10:00:00.123Z',
    dateStart: '2022-01-01 10:00:00.123Z',
    kline: combinedData,
  };

  // writeCsvFile(combinedData, outputFile);
}

combineAndSortCsvFiles();
