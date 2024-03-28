const axios = require('axios');
const moment = require('moment');
const { ValueArea } = require('./vp');

// Function to fetch OHLCV data from Binance's fAPI
async function fetchKlines(symbol, interval, limit = 500) {
  try {
    const response = await axios.get(
      `https://fapi.binance.com/fapi/v1/klines`,
      {
        params: {
          symbol: symbol,
          interval: interval,
          limit: limit,
        },
      }
    );
    return response.data.map((k) => ({
      open_time: k[0] / 1000, // Convert milliseconds to seconds
      open: parseFloat(k[1]),
      high: parseFloat(k[2]),
      low: parseFloat(k[3]),
      close: parseFloat(k[4]),
      volume: parseFloat(k[5]),
    }));
  } catch (error) {
    console.error('Error fetching data from Binance:', error);
    return [];
  }
}

// Function to group Klines into 24-hour sessions
function groupKlinesIntoSessions(klines) {
  const sessions = {};
  klines.forEach((kline) => {
    const dateKey = moment.unix(kline.open_time).utc().startOf('day').format();
    if (!sessions[dateKey]) {
      sessions[dateKey] = [];
    }
    sessions[dateKey].push(kline);
  });
  return sessions;
}

async function analyzeValueAreaBySession() {
  const klines = await fetchKlines('IMXUSDT', '15m', 1500); // Adjust limit as needed
  if (!klines.length) {
    console.log('No data available for analysis.');
    return;
  }

  const sessions = groupKlinesIntoSessions(klines);

  Object.keys(sessions).forEach((sessionDate) => {
    const sessionKlines = sessions[sessionDate];
    const valueArea = new ValueArea(24, 0.8);
    const now = moment(sessionDate);
    const { VAH, VAL, POC, low, high,POC_ROW,histogram } = valueArea.getLevelsForPeriod(
      sessionKlines,
      'day',
      now,
      false
    );

    console.log(`Session Date: ${moment(sessionDate).utcOffset(7).format()}`);

    console.log(`Value Area High (VAH): ${VAH}`);
    console.log(`Value Area Low (VAL): ${VAL}`);
    console.log(`Point of Control (POC): ${POC}`);
    console.log(`POC ROW): ${POC_ROW}`);
   //console.table(histogram.reverse());
    console.log('------------------------------------------------------');
  });
}

analyzeValueAreaBySession();
