/**
 * Time-based Playlist Scheduler (condition2)
 * 
 * Checks if current time falls within any active schedule
 * Returns matching playlist or null
 */

async function checkTimeSchedule(pool) {
  let conn;
  try {
    conn = await pool.getConnection();

    // Get current time in server timezone
    const [rows] = await conn.execute(
      `SELECT id, name, start_time, end_time, playlist_data 
       FROM schedules 
       WHERE active = true 
         AND CURTIME() BETWEEN start_time AND end_time 
       ORDER BY id 
       LIMIT 1`
    );

    if (rows.length === 0) return null;

    const schedule = rows[0];
    let playlist;
    try {
      playlist = typeof schedule.playlist_data === 'string'
        ? JSON.parse(schedule.playlist_data)
        : schedule.playlist_data;
    } catch (e) {
      console.error('❌ Invalid playlist_data in schedule:', schedule.id);
      return null;
    }

    console.log(`⏰ Schedule match: ${schedule.name} (${schedule.start_time}-${schedule.end_time})`);

    return {
      scheduleId: schedule.id,
      name: schedule.name,
      playlist: playlist,
    };
  } catch (e) {
    console.error('❌ Schedule check error:', e);
    return null;
  } finally {
    if (conn) conn.release();
  }
}

/**
 * Get all schedules (for admin)
 */
async function getAllSchedules(pool) {
  const conn = await pool.getConnection();
  try {
    const [rows] = await conn.execute(
      'SELECT * FROM schedules ORDER BY start_time'
    );
    return rows;
  } finally {
    conn.release();
  }
}

/**
 * Create/update schedule
 */
async function upsertSchedule(pool, { name, startTime, endTime, playlistData, active = true }) {
  const conn = await pool.getConnection();
  try {
    const [result] = await conn.execute(
      `INSERT INTO schedules (name, start_time, end_time, playlist_data, active)
       VALUES (?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE start_time=?, end_time=?, playlist_data=?, active=?`,
      [name, startTime, endTime, JSON.stringify(playlistData), active,
       startTime, endTime, JSON.stringify(playlistData), active]
    );
    return result;
  } finally {
    conn.release();
  }
}

// Sample schedules data
const SAMPLE_SCHEDULES = [
  { name: 'Morning Breakfast', startTime: '06:00:00', endTime: '09:00:00' },
  { name: 'Lunch Time',       startTime: '11:00:00', endTime: '14:00:00' },
  { name: 'Evening Dinner',   startTime: '17:00:00', endTime: '20:00:00' },
];

module.exports = { checkTimeSchedule, getAllSchedules, upsertSchedule, SAMPLE_SCHEDULES };
