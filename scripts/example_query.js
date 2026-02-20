use metropt;

db.sensor_data.aggregate([
  { $match: { sensor_id: "compressor_1" } },
  {
    $setWindowFields: {
      partitionBy: "$sensor_id",
      sortBy: { timestamp: 1 },
      output: {
        moving_avg_motor_current: {
          $avg: "$motor_current",
          window: { documents: [-4, 0] } // скользящее среднее на 5 значений
        }
      }
    }
  },
  { $limit: 20 } // выводим первые 20 для проверки
]).forEach(doc => printjson(doc));
