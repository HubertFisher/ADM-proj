// /scripts/find_peak_feb.js
use metropt;

const TZ = "Europe/Rome";
const DEBOUNCE_SEC = 60; // объединяем события, идущие подряд < DEBOUNCE_SEC секунд

print("Analyzing February 2020 (streaming) with debounce", DEBOUNCE_SEC, "s ...");

// match range for February 2020 in UTC
const startISO = ISODate("2020-02-01T00:00:00Z");
const endISO   = ISODate("2020-03-01T00:00:00Z");

// pipeline: project needed fields and convert numeric fields to numbers (robust against strings)
const pipeline = [
  { $match: { timestamp: { $gte: startISO, $lt: endISO } } },
  {
    $project: {
      timestamp: 1,
      day:   { $dayOfMonth: { date: "$timestamp", timezone: TZ } },
      hour:  { $hour:      { date: "$timestamp", timezone: TZ } },
      // coerce to numbers if stored as strings; if already numeric it's fine
      LPS:            { $toDouble: "$LPS" },
      Oil_level:      { $toDouble: "$Oil_level" },
      Motor_current:  { $toDouble: "$Motor_current" },
      MPG:            { $toDouble: "$MPG" },
      TP2:            { $toDouble: "$TP2" },
      COMP:           { $toDouble: "$COMP" },
      DV_eletric:     { $toDouble: "$DV_eletric" },
      DV_pressure:    { $toDouble: "$DV_pressure" },
      Oil_temperature:{ $toDouble: "$Oil_temperature" }
    }
  },
  { $sort: { timestamp: 1 } }
];

const cursor = db.sensor_data.aggregate(pipeline, { allowDiskUse: true });

// helper to determine if single document is "not normal"
function isNotNormal(doc) {
  // Use conservative checks: null/undefined treated as normal (false)
  try {
    if (doc.LPS !== null && doc.LPS !== undefined && doc.LPS > 0) return true;
    if (doc.Oil_level !== null && doc.Oil_level !== undefined && doc.Oil_level > 0) return true;
    if (doc.Motor_current !== null && doc.MPG !== null &&
        doc.Motor_current !== undefined && doc.MPG !== undefined &&
        doc.Motor_current > 8 && doc.MPG <= 0) return true;
    if (doc.TP2 !== null && doc.COMP !== null &&
        doc.TP2 !== undefined && doc.COMP !== undefined &&
        doc.TP2 < 7 && doc.COMP <= 0) return true;
    if (doc.DV_eletric !== null && doc.DV_pressure !== null &&
        doc.DV_eletric !== undefined && doc.DV_pressure !== undefined &&
        doc.DV_eletric > 0 && doc.DV_pressure !== 0) return true;
    if (doc.Oil_temperature !== null && doc.Oil_temperature !== undefined &&
        doc.Oil_temperature > 80) return true;
  } catch (e) {
    // on any unexpected type error treat as normal
  }
  return false;
}

// streaming clustering with O(1) memory + small counters
let lastTs = null;
let clusterStart = null;
let clusterEnd = null;
let clusterStartDay = null;
let clusterStartHour = null;

const dayCounts = {};           // day (1..29) -> events count
const dayHourCounts = {};       // day -> { hour -> events count }

let totalDocs = 0;
let totalClusters = 0;

cursor.forEach(doc => {
  totalDocs++;
  const tMs = doc.timestamp.getTime();
  const flag = isNotNormal(doc);

  if (flag) {
    if (clusterStart === null) {
      // open new cluster
      clusterStart = tMs;
      clusterEnd = tMs;
      clusterStartDay = doc.day;
      clusterStartHour = doc.hour;
    } else {
      // extend if within debounce
      if (tMs - lastTs <= DEBOUNCE_SEC * 1000) {
        clusterEnd = tMs;
      } else {
        // finalize previous cluster
        totalClusters++;
        const day = clusterStartDay;
        const hour = clusterStartHour;
        dayCounts[day] = (dayCounts[day] || 0) + 1;
        dayHourCounts[day] = dayHourCounts[day] || {};
        dayHourCounts[day][hour] = (dayHourCounts[day][hour] || 0) + 1;

        // start new cluster
        clusterStart = tMs;
        clusterEnd = tMs;
        clusterStartDay = doc.day;
        clusterStartHour = doc.hour;
      }
    }
  } else {
    if (clusterStart !== null) {
      // close cluster
      totalClusters++;
      const day = clusterStartDay;
      const hour = clusterStartHour;
      dayCounts[day] = (dayCounts[day] || 0) + 1;
      dayHourCounts[day] = dayHourCounts[day] || {};
      dayHourCounts[day][hour] = (dayHourCounts[day][hour] || 0) + 1;

      clusterStart = null;
      clusterEnd = null;
      clusterStartDay = null;
      clusterStartHour = null;
    }
  }

  lastTs = tMs;
});

// finalize trailing cluster
if (clusterStart !== null) {
  totalClusters++;
  const day = clusterStartDay;
  const hour = clusterStartHour;
  dayCounts[day] = (dayCounts[day] || 0) + 1;
  dayHourCounts[day] = dayHourCounts[day] || {};
  dayHourCounts[day][hour] = (dayHourCounts[day][hour] || 0) + 1;
  clusterStart = null;
}

// find day with max events
let maxDay = null;
let maxDayCount = -1;
Object.keys(dayCounts).forEach(k => {
  const c = dayCounts[k];
  if (c > maxDayCount) { maxDayCount = c; maxDay = parseInt(k,10); }
});

if (maxDay === null) {
  print("No anomaly clusters found in February 2020.");
} else {
  print(`February 2020: day with most anomaly events -> ${maxDay} (events: ${maxDayCount})`);

  // find hour in that day with max events
  const hoursMap = dayHourCounts[maxDay] || {};
  let maxHour = null;
  let maxHourCount = -1;
  Object.keys(hoursMap).forEach(h => {
    const cnt = hoursMap[h];
    if (cnt > maxHourCount) { maxHourCount = cnt; maxHour = parseInt(h,10); }
  });

  if (maxHour === null) {
    print(`No per-hour events found for day ${maxDay}.`);
  } else {
    print(`On ${maxDay} Feb 2020 the worst hour is ${String(maxHour).padStart(2,"0")}:00 with ${maxHourCount} anomaly events (debounced).`);
  }

  // optional: print top 5 days
  print("\nTop days in Feb by anomaly event count:");
  const dayList = Object.keys(dayCounts).map(d => ({ day: parseInt(d,10), cnt: dayCounts[d] }));
  dayList.sort((a,b) => b.cnt - a.cnt);
  dayList.slice(0,10).forEach(r => print(`Day ${r.day}: ${r.cnt} events`));
}

print("\nTotal scanned docs:", totalDocs, " total clusters found:", totalClusters);
print("Done.");
