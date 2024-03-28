#!/usr/bin/env node
const axios = require('axios');

const readline = require('readline');
const fastCsv = require('fast-csv');
const csvParser = require('csv-parser');
const AdmZip = require('adm-zip');
const xml2js = require('xml2js');
const yargs = require('yargs/yargs');
const { hideBin } = require('yargs/helpers');

const fs = require('fs');
const path = require('path');
const argv = yargs(hideBin(process.argv))
  .usage('Usage: $0 --timeframe [string] --symbol [string]')
  .example(
    '$0 --timeframe 15m --symbol ZENUSDT',
    'List all .zip files for ZENUSDT with a 15m timeframe'
  )
  .option('timeframe', {
    alias: 't',
    describe: 'Set the timeframe for the data',
    type: 'string',
    demandOption: true,
    default: '15m',
  })
  .option('symbol', {
    alias: 's',
    describe: 'Set the symbol for the data',
    type: 'string',
    demandOption: true,
    default: 'IMXUSDT',
  })
  .help('h')
  .alias('h', 'help').argv;

const fetchBucketUrl = (timeframe, symbol) => {
  // This URL is used to fetch the XML with the listing of files
  const base = 'https://s3-ap-northeast-1.amazonaws.com/data.binance.vision/';
  const path = `?prefix=data/futures/um/monthly/klines/${symbol}/${timeframe}`;
  return `${base}${path}`;
};

const constructDownloadUrl = (key) => {
  // This URL is the base for downloading the actual files
  return `https://data.binance.vision/${key}`;
};

const listZipFiles = async (url) => {
  try {
    const response = await axios.get(url);
    const parser = new xml2js.Parser({
      explicitArray: false,
      ignoreAttrs: true,
    });
    const result = await parser.parseStringPromise(response.data);

    const files = result.ListBucketResult.Contents.filter((item) =>
      item.Key.endsWith('.zip')
    ).map((item) => constructDownloadUrl(item.Key));

    if (files.length === 0) {
      console.log('No .zip files found.');
      return;
    }
    console.log(files);
    return files;
  } catch (error) {
    console.error('Failed to fetch or parse XML:', error.message);
  }
};
var tf;
var symbol;
const bucketUrl = fetchBucketUrl(argv.timeframe, argv.symbol);
tf = argv.timeframe;
symbol = argv.symbol;
listZipFiles(bucketUrl).then(async (data) => {
  console.log('Có link, bắt đầu download');
  // console.log(data);
  downloadAndExtractZipFiles(data).then(async (data) => {
    console.log('download complete.');
    await combineAndSortCsvFiles();
  });
});

async function downloadAndExtractZipFiles(urls) {
  // Ensure the data directory exists
  const dataDir = path.join(__dirname, 'data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  for (let url of urls) {
    try {
      // Extract the file name from the URL
      const fileName = url.split('/').pop();
      const filePath = path.join(dataDir, fileName);

      // Download the file
      console.log(`- Downloading ${url}...`);
      const response = await axios({
        method: 'get',
        url: url,
        responseType: 'arraybuffer',
      });

      // Save the zip file to the filesystem
      fs.writeFileSync(filePath, response.data);
      console.log(`Saved ${fileName}`);

      // Extract the zip file
      console.log(`Extracting ${fileName}...`);
      const zip = new AdmZip(filePath);
      zip.extractAllTo(dataDir, true);
      console.log(`Extracted ${fileName}`);

      // Optionally, delete the zip file after extraction
      fs.unlinkSync(filePath);
      console.log(`Deleted ${fileName}`);
    } catch (error) {
      console.error(`Failed to download and extract ${url}: ${error}`);
    }
  }
}

const dataDir = path.join(__dirname, 'data');
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
    .on('finish', () => {
      console.log(`Combined CSV written to ${filePath}`);
      //delete all file in data
      deleteAllFilesInDirectory('data');
      console.log('delete all file');
      removeSecondLine(filePath, filePath + '_temp');
    });
}

function deleteAllFilesInDirectory(directory) {
  // Read all files in the directory
  fs.readdir(directory, (err, files) => {
    if (err) throw err;

    for (const file of files) {
      const filePath = path.join(directory, file);
      fs.stat(filePath, (err, stat) => {
        if (err) throw err;

        if (stat.isFile()) {
          // Delete the file
          fs.unlink(filePath, (err) => {
            if (err) throw err;
            // console.log(`Deleted ${filePath}`);
          });
        }
      });
    }
  });
}
async function combineAndSortCsvFiles() {
  const combinedData = await readCsvFiles(dataDir);

  const outputFile = path.join(__dirname, `klines/${symbol}_${tf}.csv`);
  writeCsvFile(combinedData, outputFile);
}

async function removeSecondLine(filePath, tempFilePath) {
  const fileStream = fs.createReadStream(filePath);
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  const outputStream = fs.createWriteStream(tempFilePath);
  let lineNumber = 0;

  for await (const line of rl) {
    lineNumber++;
    // Skip the second line
    if (lineNumber === 2) {
      continue;
    }
    outputStream.write(`${line}\n`);
  }

  outputStream.close();
  fileStream.close();

  // After processing, close the streams and replace the original file with the corrected one
  outputStream.on('finish', () => {
    // Replace the original file with the temp file
    fs.rename(tempFilePath, filePath, (err) => {
      if (err) throw err;
      console.log('The second line has been successfully removed.');
    });
  });
}
