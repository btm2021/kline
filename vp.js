const moment = require('moment');
function countDecimals(value) {
  let text = value.toString();
  // verify if number 0.000005 is represented as "5e-6"
  if (text.indexOf('e-') > -1) {
    let [base, trail] = text.split('e-');
    let deg = parseInt(trail, 10);
    return deg;
  }
  // count decimals for number in representation like "0.123456"
  if (Math.floor(value) !== value) {
    return value.toString().split('.')[1].length || 0;
  }
  return 0;
}

function round(number, precision = 2) {
  if (precision < 0) {
    const factor = Math.pow(10, precision);
    return Math.round(number * factor) / factor;
  } else {
    return +(Math.round(Number(number + 'e+' + precision)) + 'e-' + precision);
  }
}

class ValueArea {
  nRows = 24;
  static VA_VOL_PERCENT = 0.8;
  constructor(nRows = 24, VA_VOL_PERCENT = 0.8) {
    moment.updateLocale('en', {
      week: {
        dow: 1, // Monday is the first day of the week.
      },
    });
    this.VA_VOL_PERCENT = VA_VOL_PERCENT;
    this.nRows = nRows;
  }
  sumVolumes(klines) {
    let V_TOTAL = 0;
    let highest = 0;
    let lowest = Infinity;

    for (let i = 0; i < klines.length; i++) {
      const { volume, high, low } = klines[i];
      V_TOTAL += volume;

      if (high > highest) highest = high;
      if (low < lowest) lowest = low;
    }

    return { V_TOTAL: round(V_TOTAL), highest, lowest };
  }

  valueAreaHistogram(klines, highest, lowest) {
    let row = 0;
    const range = highest - lowest;
    const nDecimals = Math.max(countDecimals(highest), countDecimals(lowest));
    const stepSize = round(range / this.nRows, nDecimals);

    const histogram = [];
    let POC_ROW = 0;
    let POC = 0;
    let highestVolumeRow = 0;
    while (histogram.length < this.nRows) {
      histogram.push({
        volume: 0,
        low: round(lowest + stepSize * row, nDecimals),
        mid: round(lowest + stepSize * row + stepSize / 2, nDecimals),
        high: round(lowest + stepSize * row + stepSize, nDecimals),
      });
      row++;
    }

    for (let i = 0; i < klines.length; i++) {
      const { volume, close, open, high, low } = klines[i];
      const avg = (high + low) / 2;
      const ROW = Math.min(
        this.nRows - 1,
        Math.floor((avg - lowest) / stepSize)
      );
      histogram[ROW].volume += volume;

      if (histogram[ROW].volume > highestVolumeRow) {
        highestVolumeRow = histogram[ROW].volume;
        POC = histogram[ROW].mid;
        POC_ROW = ROW;
      }
    }
    return { histogram, POC, POC_ROW };
  }

  calcValueArea(POC_ROW, histogram, V_TOTAL) {
    // 70% of the total volume
    const VA_VOL = V_TOTAL * ValueArea.VA_VOL_PERCENT;

    // Set the upper / lower indices to the POC row to begin with
    // They will move up / down the histogram when adding the volumes
    let lowerIndex = POC_ROW;
    let upperIndex = POC_ROW;

    // The histogram bars
    const bars = histogram.length - 1;

    // The volume area starts with the POC volume
    let volumeArea = histogram[POC_ROW].volume;

    function isTargetVolumeReached() {
      return volumeArea >= VA_VOL;
    }

    function getNextLowerBar() {
      return lowerIndex > 0 ? histogram[--lowerIndex].volume : 0;
    }

    function getNextHigherBar() {
      return upperIndex < bars ? histogram[++upperIndex].volume : 0;
    }

    function getDualPrices(goUp) {
      return goUp
        ? getNextHigherBar() + getNextHigherBar()
        : getNextLowerBar() + getNextLowerBar();
    }

    function isAtBottomOfHistogram() {
      return lowerIndex <= 0;
    }

    function isAtTopOfHistogram() {
      return upperIndex >= bars;
    }

    function isAllBarsVisited() {
      return isAtBottomOfHistogram() && isAtTopOfHistogram();
    }

    do {
      const remainingLowerBars = Math.min(Math.abs(0 - lowerIndex), 2);
      const remainingUpperBars = Math.min(Math.abs(bars - upperIndex), 2);
      const lowerDualPrices = getDualPrices(false);
      const higherDualPrices = getDualPrices(true);

      if (lowerDualPrices > higherDualPrices) {
        volumeArea += lowerDualPrices;
        if (!isAtTopOfHistogram() || remainingUpperBars) {
          // Upper dual prices aren't used, go back to original position
          upperIndex = Math.min(bars, upperIndex - remainingUpperBars);
        }
      } else if (higherDualPrices > lowerDualPrices) {
        volumeArea += higherDualPrices;
        if (!isAtBottomOfHistogram() || remainingLowerBars) {
          // Lower dual prices aren't used, go back to original position
          lowerIndex = Math.max(0, lowerIndex + remainingLowerBars);
        }
      }
    } while (!isTargetVolumeReached() || isAllBarsVisited());

    const VAL = histogram[lowerIndex].low;
    const VAH = histogram[upperIndex].high;
    return { VAH, VAL };
  }

  getLevelsForPeriod(data, period, currentPeriod, goToPreviousPeriod) {
    // We need to start at the start of the (day / week / month), in order to filter all the klines for the VA calculations for that period
    // current day vs previous day, current week vs previous week, current month vs previous month
    const from = goToPreviousPeriod
      ? currentPeriod.subtract(1, period).startOf(period)
      : currentPeriod.startOf(period);
    const periodicKlines = data.filter(({ open_time }) =>
      moment(open_time * 1000).isSame(from, period)
    );

    const { V_TOTAL, highest, lowest } = this.sumVolumes(periodicKlines);
    const { histogram, POC, POC_ROW } = this.valueAreaHistogram(
      periodicKlines,
      highest,
      lowest
    );
    const { VAH, VAL } = this.calcValueArea(POC_ROW, histogram, V_TOTAL);

    return { VAH, VAL, POC, low: lowest, high: highest, POC_ROW, histogram };
  }
}

module.exports = { ValueArea };
