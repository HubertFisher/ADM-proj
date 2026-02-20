use metropt;

db.sensor_data.aggregate([
  { $match: { COMP: 1.0 } },
  {
    $setWindowFields: {
      partitionBy: "$COMP",
      sortBy: { timestamp: 1 },
      output: {
        moving_avg_motor_current: {
          $avg: "$Motor_current",
          window: { documents: [-4, 0] }
        }
      }
    }
  },
  { $limit: 20 }
]).forEach(doc => printjson(doc));
