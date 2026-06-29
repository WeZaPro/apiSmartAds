require("dotenv").config();
const express = require("express");
const http = require("http");
const mqtt = require("mqtt");
const cors = require("cors");
const mysql = require("mysql2/promise");
const { v4: uuidv4 } = require("uuid");
const path = require("path");
const multer = require("multer");
const fs = require("fs");
const { checkTimeSchedule } = require("./services/scheduler");

const app = express();
const server = http.createServer(app);

app.use(cors());
app.use(express.json({ limit: "10mb" }));
app.use(express.static(path.join(__dirname, "..", "public")));
app.use("/videos", express.static(path.join(__dirname, "..", "videos")));
app.use("/uploads", express.static(path.join(__dirname, "..", "uploads")));

// ============ UPLOAD CONFIG ============
const uploadDirs = {
  videos: path.join(__dirname, "..", "videos"),
  covers: path.join(__dirname, "..", "uploads", "covers"),
  promos: path.join(__dirname, "..", "uploads", "promos"),
  menuCover: path.join(__dirname, "..", "uploads", "menuCover"),
};

Object.values(uploadDirs).forEach((dir) => {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    if (file.fieldname === "video") cb(null, uploadDirs.videos);
    else if (file.fieldname === "image_cover") cb(null, uploadDirs.covers);
    else if (file.fieldname === "image_promo") cb(null, uploadDirs.promos);
    else if (file.fieldname === "cover_image") cb(null, uploadDirs.menuCover);
    else cb(null, uploadDirs.videos);
  },
  filename: (req, file, cb) => {
    // ใช้ timestamp เดียวกันทั้ง request
    if (!req._uploadTimestamp) req._uploadTimestamp = Date.now();
    cb(null, `${req._uploadTimestamp}_${file.originalname}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 500 * 1024 * 1024 },
  fileFilter: (req, file, cb) => {
    if (file.fieldname === "video" && !file.mimetype.startsWith("video/")) {
      return cb(new Error("Only video files allowed"));
    }
    if (
      (file.fieldname === "image_cover" || file.fieldname === "image_promo") &&
      !file.mimetype.startsWith("image/")
    ) {
      return cb(new Error("Only image files allowed"));
    }
    cb(null, true);
  },
});

// ============ MQTT CONFIG ============
// const ENV = process.env.NODE_ENV || "DEV";
const ENV = process.env.NODE_ENV || "PRODUCTION";
const SKIP_DB = process.env.SKIP_DB === "true";

const MQTT_CONFIG = {
  DEV: {
    url: "mqtt://test.mosquitto.org:1883",
    options: { clientId: `smartads_server_${Date.now()}` },
  },
  PRODUCTION: {
    url: `mqtt://${process.env.MQTT_HOST || "27.254.143.113"}:${
      process.env.MQTT_PORT || 9359
    }`,
    options: {
      clientId: `smartads_server_${Date.now()}`,
      username: process.env.MQTT_USER || "",
      password: process.env.MQTT_PASS || "",
    },
  },
};

const mqttConfig = MQTT_CONFIG[ENV] || MQTT_CONFIG.DEV;
console.log(`🔌 MQTT ENV: ${ENV} → ${mqttConfig.url}`);

const mqttClient = mqtt.connect(mqttConfig.url, mqttConfig.options);

// ============ DATABASE ============
// ============ DATABASE ============
let pool = null;
if (!SKIP_DB) {
  const DB_CONFIG = {
    DEV: {
      host: process.env.DB_HOST || "localhost",
      user: process.env.DB_USER || "root",
      password: process.env.DB_PASSWORD || "",
      database: process.env.DB_NAME || "smart_ads",
      connectionLimit: 10,
    },
    PRODUCTION: {
      host: process.env.DB_HOST || "localhost",
      user: process.env.DB_USER || "happyevvip",
      password: process.env.DB_PASSWORD || "Taweesak@5050",
      database: process.env.DB_NAME || "smart_ads",
      connectionLimit: 10,
    },
  };

  pool = mysql.createPool(DB_CONFIG[ENV] || DB_CONFIG.DEV);
  console.log(`🗄️ DB pool created (${ENV})`);
} else {
  console.log("⚠️ DB skipped (SKIP_DB=true)");
}

async function dbQuery(sql, params = []) {
  if (!pool) return null;
  let conn;
  try {
    conn = await pool.getConnection();
    const [result] = await conn.execute(sql, params);
    return result;
  } catch (e) {
    console.error("❌ DB error:", e.message);
    return null;
  } finally {
    if (conn) conn.release();
  }
}

// ============ HELPERS ============
function publish(tabletId, event, data) {
  const topic = `sma/t/${tabletId}/dn/${event}`;
  mqttClient.publish(topic, JSON.stringify(data), { qos: 1 });
}

const connectedTablets = {};
const pendingRequests = {};

function handlePendingResponse(tabletId, type, data) {
  const key = `${tabletId}_${type}`;
  if (pendingRequests[key]) {
    pendingRequests[key](data);
    delete pendingRequests[key];
  }
}

function waitForResponse(tabletId, type, timeoutMs = 10000) {
  return new Promise((resolve) => {
    const key = `${tabletId}_${type}`;
    const timer = setTimeout(() => {
      delete pendingRequests[key];
      resolve(null);
    }, timeoutMs);
    pendingRequests[key] = (data) => {
      clearTimeout(timer);
      resolve(data);
    };
  });
}

// ============ GEO-FENCE ============
async function checkGeofence(lat, lng) {
  const rows = await dbQuery(
    `SELECT *, (6371 * acos(cos(radians(?)) * cos(radians(lat)) * cos(radians(lng) - radians(?)) + sin(radians(?)) * sin(radians(lat)))) AS dist
     FROM areas WHERE active = true HAVING dist <= radius_km ORDER BY dist LIMIT 1`,
    [lat, lng, lat]
  );
  return rows && rows.length > 0 ? rows[0] : null;
}

// ============ MQTT CONNECTION ============
mqttClient.on("connect", () => {
  console.log("✅ MQTT Connected to", mqttConfig.url);
  mqttClient.subscribe("sma/t/+/up/#", (err) => {
    if (err) console.error("❌ Subscribe error:", err);
    else console.log("📡 Subscribed to sma/t/+/up/#");
  });
});

mqttClient.on("error", (err) => console.error("❌ MQTT Error:", err));
mqttClient.on("reconnect", () => console.log("🔄 MQTT Reconnecting..."));

// ============ MQTT MESSAGE HANDLER ============
mqttClient.on("message", async (topic, message) => {
  try {
    const parts = topic.split("/");
    if (
      parts.length < 5 ||
      parts[0] !== "sma" ||
      parts[1] !== "t" ||
      parts[3] !== "up"
    )
      return;

    const tabletId = parts[2];
    const event = parts[4];
    let data;
    try {
      data = JSON.parse(message.toString());
    } catch {
      data = message.toString();
    }

    const now = new Date();
    const tabletTime = data?.time || now.toISOString();

    connectedTablets[tabletId] = {
      ...connectedTablets[tabletId],
      lastSeen: now,
      status: "active",
    };

    switch (event) {
      case "register":
        console.log(`📱 Tablet registered: ${tabletId}`);
        connectedTablets[tabletId] = {
          lastSeen: now,
          status: "active",
          ...data,
        };
        await dbQuery(
          `INSERT INTO tablets (tablet_id, status, current_playlist_type, last_seen) VALUES (?,?,'default',NOW())
           ON DUPLICATE KEY UPDATE status='active', last_seen=NOW()`,
          [tabletId, "active"]
        );

        // Auto insert state_change = active (กันกรณี wakeup_request ไม่ถึง)
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, screen_state, tablet_time) VALUES (?,?,?,?)`,
          [tabletId, "state_change", "active", tabletTime]
        );

        publish(tabletId, "registered", {
          status: "ok",
          tabletId,
          serverTime: now.toISOString(),
        });

        // Check schedule/geofence on register (กรณี wakeup_request ไม่ถึง)
        try {
          let regPlaylist = null;
          const sched = await checkTimeSchedule(pool);
          if (sched) {
            regPlaylist = {
              type: "condition",
              playlist: sched.playlist,
              source: "schedule",
              scheduleName: sched.name,
            };
          }
          if (regPlaylist) {
            publish(tabletId, "update_playlist", regPlaylist);
            await dbQuery(
              "UPDATE tablets SET current_playlist_type=? WHERE tablet_id=?",
              [regPlaylist.type, tabletId]
            );
            console.log(
              `📤 Playlist sent to ${tabletId} on register (source: ${regPlaylist.source})`
            );
          }
        } catch (e) {
          console.error("Schedule check on register error:", e.message);
        }

        break;

      case "heartbeat": {
        const lat = data.location?.lat || null;
        const lng = data.location?.lng || null;
        const accuracy = data.location?.accuracy || null;

        connectedTablets[tabletId] = {
          ...connectedTablets[tabletId],
          lastSeen: now,
          battery: data.battery,
          status: data.status,
          location: data.location,
          playlist: data.playlist,
          hasPerson: data.hasPerson,
          attention: data.attention,
        };

        console.log(
          `💓 ${tabletId}: bat=${data.battery}% status=${data.status} video=${
            data.playlist || "-"
          } person=${data.hasPerson} pos=${
            lat ? lat.toFixed(4) + "," + lng.toFixed(4) : "null"
          } mem=${data.memoryMB || "-"}MB temp=${data.temperature || "-"}°C`
        );

        // Temperature warning
        if (data.temperature && data.temperature > 45) {
          console.log(
            `🔥 WARNING: ${tabletId} temperature=${data.temperature}°C > 45°C!`
          );
        }
        // Memory warning
        if (data.memoryMB && data.memoryMB > 700) {
          console.log(
            `⚠️ WARNING: ${tabletId} memory=${data.memoryMB}MB > 700MB!`
          );
        }
        // Memory critical → reboot
        if (data.memoryMB && data.memoryMB > 1000) {
          console.log(
            `🚨 CRITICAL: ${tabletId} memory=${data.memoryMB}MB → reboot!`
          );
          publish(tabletId, "command", { type: "reboot" });
        }

        // Update tablets (INSERT ON DUPLICATE KEY → กันกรณี row ไม่มี)
        await dbQuery(
          `INSERT INTO tablets (tablet_id, battery_level, status, lat, lng,
           current_video, has_person, ble_status, mqtt_status, memory_mb, temperature, last_seen)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,NOW())
           ON DUPLICATE KEY UPDATE
           battery_level=VALUES(battery_level), status=VALUES(status),
           lat=VALUES(lat), lng=VALUES(lng),
           current_video=VALUES(current_video), has_person=VALUES(has_person),
           ble_status=VALUES(ble_status), mqtt_status=VALUES(mqtt_status),
           memory_mb=VALUES(memory_mb), temperature=VALUES(temperature),
           current_playlist_type=IF(VALUES(current_playlist_type) IS NOT NULL, VALUES(current_playlist_type), current_playlist_type),
           last_seen=NOW()`,
          [
            tabletId,
            data.battery,
            data.status,
            lat,
            lng,
            data.playlist,
            data.hasPerson ? 1 : 0,
            data.ble || "disconnected",
            data.mqtt ? "connected" : "disconnected",
            data.memoryMB || null,
            data.temperature || null,
          ]
        );

        // Insert heartbeat log
        await dbQuery(
          `INSERT INTO tablet_heartbeat_logs
           (tablet_id, battery_level, status, lat, lng, accuracy, current_video,
            has_person, attention, ble_status, mqtt_status, memory_mb, temperature, tablet_time)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
          [
            tabletId,
            data.battery,
            data.status,
            lat,
            lng,
            accuracy,
            data.playlist,
            data.hasPerson,
            data.attention,
            data.ble,
            data.mqtt,
            data.memoryMB || null,
            data.temperature || null,
            tabletTime,
          ]
        );

        // Update playlist type from heartbeat
        if (data.playlistType) {
          await dbQuery(
            "UPDATE tablets SET current_playlist_type=? WHERE tablet_id=?",
            [data.playlistType, tabletId]
          );
        }

        break;
      }

      case "state": {
        const { state } = data;
        console.log(`📱 ${tabletId} state → ${state}`);
        if (connectedTablets[tabletId])
          connectedTablets[tabletId].status = state;

        await dbQuery(
          "UPDATE tablets SET status=?, last_seen=NOW() WHERE tablet_id=?",
          [state === "sleep" ? "sleeping" : "active", tabletId]
        );

        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, screen_state, tablet_time) VALUES (?,?,?,?)`,
          [tabletId, "state_change", state, tabletTime]
        );
        break;
      }

      case "wakeup_request": {
        const { location } = data;
        let playlist = null;

        const info = connectedTablets[tabletId] || {};
        await dbQuery(
          `INSERT INTO tablet_status_logs 
           (tablet_id, event, battery_level, screen_state, current_video, lat, lng, extra, tablet_time)
           VALUES (?,?,?,?,?,?,?,?,?)`,
          [
            tabletId,
            "wakeup_request",
            info.battery || null,
            "wakeup",
            info.playlist || null,
            location?.lat || null,
            location?.lng || null,
            JSON.stringify({
              accuracy: location?.accuracy,
              condition: data.condition,
            }),
            tabletTime,
          ]
        );

        if (location?.lat) {
          await dbQuery(
            "UPDATE tablets SET lat=?, lng=?, last_seen=NOW() WHERE tablet_id=?",
            [location.lat, location.lng, tabletId]
          );
        }

        if (location?.lat && location?.lng) {
          const area = await checkGeofence(location.lat, location.lng);
          if (area?.playlist_data) {
            const pl =
              typeof area.playlist_data === "string"
                ? JSON.parse(area.playlist_data)
                : area.playlist_data;
            playlist = {
              type: "condition",
              playlist: pl,
              source: "geofence",
              areaName: area.name,
            };
            console.log(`📍 Geo-fence match: ${area.name}`);
          }
        }

        if (!playlist) {
          const sched = await checkTimeSchedule(pool);
          if (sched) {
            playlist = {
              type: "condition",
              playlist: sched.playlist,
              source: "schedule",
              scheduleName: sched.name,
            };
            console.log(`⏰ Schedule match: ${sched.name}`);
          }
        }

        if (playlist) {
          publish(tabletId, "update_playlist", playlist);
          await dbQuery(
            "UPDATE tablets SET current_playlist_type=?, last_seen=NOW() WHERE tablet_id=?",
            [playlist.type || "condition", tabletId]
          );
          console.log(
            `📤 Playlist sent to ${tabletId} (source: ${playlist.source}) → DB: ${playlist.type}`
          );
        }
        publish(tabletId, "wakeup_ack", {
          status: "ok",
          hasCondition: !!playlist,
        });

        // Auto insert state_change = active
        // Auto insert state_change = active + update playlist type
        const plType = playlist ? playlist.type || "condition" : "default";
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, screen_state, tablet_time) VALUES (?,?,?,?)`,
          [tabletId, "state_change", "active", tabletTime]
        );
        await dbQuery(
          "UPDATE tablets SET status='active', current_playlist_type=?, last_seen=NOW() WHERE tablet_id=?",
          [plType, tabletId]
        );

        break;
      }

      case "attention_minute":
        console.log(
          `📊 ${tabletId}: attention=${data.attentionPercent}% video=${data.video} (${data.lookingSeconds}s/${data.totalSeconds}s)`
        );
        await dbQuery(
          `INSERT INTO tablet_attention_logs
           (tablet_id, attention_percent, looking_seconds, total_seconds, video_name, minute_start, tablet_time)
           VALUES (?,?,?,?,?,?,?)`,
          [
            tabletId,
            data.attentionPercent,
            data.lookingSeconds,
            data.totalSeconds,
            data.video,
            data.minuteStart,
            tabletTime,
          ]
        );
        await dbQuery(
          "UPDATE tablets SET current_video=?, last_seen=NOW() WHERE tablet_id=?",
          [data.video, tabletId]
        );
        break;

      case "attention_summary":
        console.log(
          `📊 Attention summary from ${tabletId}: avg=${data?.summary?.avgPercent}%`
        );
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, attention_score, extra, tablet_time) VALUES (?,?,?,?,?)`,
          [
            tabletId,
            "attention_summary",
            data?.summary?.avgPercent,
            JSON.stringify(data?.summary),
            tabletTime,
          ]
        );
        break;

      case "attention":
        if (connectedTablets[tabletId]) {
          connectedTablets[tabletId].hasPerson = data.hasPerson;
          connectedTablets[tabletId].attention = data.attention;
          connectedTablets[tabletId].currentVideo = data.video;
        }
        break;

      case "action":
        console.log(
          `👆 ${tabletId} action: ${data.action} video=${data.video}`
        );
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, current_video, extra, tablet_time) VALUES (?,?,?,?,?)`,
          [
            tabletId,
            `action_${data.action}`,
            data.video,
            JSON.stringify(data.extra),
            tabletTime,
          ]
        );
        break;

      case "videos":
        handlePendingResponse(tabletId, "videos", data);
        break;

      case "playlist_data":
        handlePendingResponse(tabletId, "playlist_data", data);
        break;

      case "download_ack":
        console.log(
          `✅ Download ACK from ${tabletId}:`,
          JSON.stringify(data).substring(0, 200)
        );
        handlePendingResponse(tabletId, "download_ack", data);
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, extra, tablet_time) VALUES (?,?,?,?)`,
          [tabletId, "download_complete", JSON.stringify(data), tabletTime]
        );
        break;

      case "download_progress":
        console.log(`📥 ${tabletId}: ${data.file} → ${data.progress}%`);
        break;

      case "offline":
        console.log(`📴 ${tabletId} OFFLINE (Last Will)`);
        if (connectedTablets[tabletId]) {
          connectedTablets[tabletId].status = "offline";
          connectedTablets[tabletId].lastSeen = now;
        }
        await dbQuery(
          "UPDATE tablets SET status='offline', last_seen=NOW() WHERE tablet_id=?",
          [tabletId]
        );
        await dbQuery(
          `INSERT INTO tablet_status_logs (tablet_id, event, tablet_time) VALUES (?,?,?)`,
          [tabletId, "offline", tabletTime]
        );
        break;

      default:
        console.log(
          `📩 ${tabletId}/${event}:`,
          JSON.stringify(data).substring(0, 100)
        );
    }
  } catch (e) {
    console.error("❌ Message handler error:", e.message);
  }
});

// ============ STALE TABLET CHECK ============
// ============ STALE TABLET CHECK ============
setInterval(async () => {
  const now = Date.now();

  // 1. Check in-memory tablets
  for (const [id, info] of Object.entries(connectedTablets)) {
    const diff = now - (info.lastSeen?.getTime() || 0);
    if (diff > 120000 && info.status !== "offline") {
      console.log(
        `⚠️ ${id} no heartbeat ${Math.round(diff / 1000)}s → offline`
      );
      connectedTablets[id].status = "offline";
    }
    if (diff > 86400000) delete connectedTablets[id];
  }

  // 2. Check DB — mark offline ถ้า last_seen เก่ากว่า 2 นาที
  const result = await dbQuery(
    `UPDATE tablets SET status='offline'
     WHERE status IN ('active','sleeping')
     AND last_seen < NOW() - INTERVAL 2 MINUTE`
  );
  if (result && result.affectedRows > 0) {
    console.log(
      `📴 DB: ${result.affectedRows} tablet(s) marked offline (stale)`
    );
  }
}, 60000);

// สั่งให้เมื่อเปิดหน้าแรก (/) แล้วให้ส่งไฟล์ index.html กลับไปที่เบราว์เซอร์
app.get("/", (req, res) => {
  res.sendFile(path.join(__dirname, "templates", "index.html"));
});

// ============ REST API: HEALTH ============
app.get("/health", (_, res) =>
  res.json({
    status: "ok",
    mqtt: mqttClient.connected ? "connected" : "disconnected",
    env: ENV,
    broker: mqttConfig.url,
    db: pool ? "connected" : "skipped",
  })
);

// ============ REST API: TABLETS ============
app.get("/api/tablets/connected", (_, res) => {
  const list = Object.entries(connectedTablets).map(([id, info]) => ({
    tabletId: id,
    lastSeen: info.lastSeen,
    status: info.status,
    battery: info.battery,
    location: info.location,
    playlist: info.playlist,
    hasPerson: info.hasPerson,
    attention: info.attention,
  }));
  res.json({ connected: list, count: list.length });
});

app.get("/api/dashboard/stats", async (_, res) => {
  if (!pool) {
    const active = Object.values(connectedTablets).filter(
      (t) => t.status === "active"
    ).length;
    const sleeping = Object.values(connectedTablets).filter(
      (t) => t.status === "sleeping"
    ).length;
    const offline = Object.values(connectedTablets).filter(
      (t) => t.status === "offline"
    ).length;
    return res.json({
      total: Object.keys(connectedTablets).length,
      active,
      sleeping,
      offline,
      source: "memory",
    });
  }
  const total = await dbQuery("SELECT COUNT(*) as c FROM tablets");
  const active = await dbQuery(
    'SELECT COUNT(*) as c FROM tablets WHERE status="active"'
  );
  const sleeping = await dbQuery(
    'SELECT COUNT(*) as c FROM tablets WHERE status="sleeping"'
  );
  const offline = await dbQuery(
    'SELECT COUNT(*) as c FROM tablets WHERE status="offline"'
  );
  res.json({
    total: total?.[0]?.c || 0,
    active: active?.[0]?.c || 0,
    sleeping: sleeping?.[0]?.c || 0,
    offline: offline?.[0]?.c || 0,
    source: "db",
  });
});

app.get("/api/tablets/all", async (_, res) => {
  if (!pool)
    return res.json({
      tablets: Object.entries(connectedTablets).map(([id, info]) => ({
        tablet_id: id,
        ...info,
      })),
    });
  const rows = await dbQuery("SELECT * FROM tablets ORDER BY last_seen DESC");
  res.json({ tablets: rows || [] });
});

app.get("/api/tablet/:tabletId/videos", async (req, res) => {
  const { tabletId } = req.params;
  if (!connectedTablets[tabletId])
    return res.json({
      error: "Tablet not connected",
      connectedTablets: Object.keys(connectedTablets),
    });
  publish(tabletId, "request_videos", {});
  const data = await waitForResponse(tabletId, "videos");
  if (!data) return res.json({ error: "Tablet not responding (timeout 10s)" });
  res.json({
    tabletId,
    videos: data.videos || [],
    count: data.count || 0,
    time: data.time,
  });
});

app.get("/api/tablet/:tabletId/playlist", async (req, res) => {
  const { tabletId } = req.params;
  if (!connectedTablets[tabletId])
    return res.json({ error: "Tablet not connected" });
  publish(tabletId, "request_playlist", {});
  const data = await waitForResponse(tabletId, "playlist_data");
  if (!data) return res.json({ error: "Tablet not responding (timeout 10s)" });
  res.json({
    tabletId,
    config: data.config,
    currentPlaylist: data.playlist,
    playlistType: data.type,
    time: data.time,
  });
});

app.get("/api/tablets/overview", async (req, res) => {
  const tablets = Object.keys(connectedTablets);
  if (tablets.length === 0) return res.json({ tablets: [], count: 0 });
  const results = [];
  for (const tabletId of tablets) {
    publish(tabletId, "request_videos", {});
    const data = await waitForResponse(tabletId, "videos", 5000);
    results.push(
      data
        ? { tabletId, videos: data.videos || [], count: data.count || 0 }
        : { tabletId, error: "timeout" }
    );
  }
  res.json({ tablets: results, count: results.length });
});

app.get("/api/tablet/:tabletId/heartbeat-logs", async (req, res) => {
  const { tabletId } = req.params;
  const limit = parseInt(req.query.limit) || 60;
  const rows = await dbQuery(
    "SELECT * FROM tablet_heartbeat_logs WHERE tablet_id=? ORDER BY recorded_at DESC LIMIT ?",
    [tabletId, limit]
  );
  res.json({ tabletId, logs: rows || [], count: rows?.length || 0 });
});

app.get("/api/tablet/:tabletId/attention-logs", async (req, res) => {
  const { tabletId } = req.params;
  const limit = parseInt(req.query.limit) || 60;
  const rows = await dbQuery(
    "SELECT * FROM tablet_attention_logs WHERE tablet_id=? ORDER BY recorded_at DESC LIMIT ?",
    [tabletId, limit]
  );
  res.json({ tabletId, logs: rows || [], count: rows?.length || 0 });
});

app.get("/api/tablet/:tabletId/status-logs", async (req, res) => {
  const { tabletId } = req.params;
  const limit = parseInt(req.query.limit) || 100;
  const rows = await dbQuery(
    "SELECT * FROM tablet_status_logs WHERE tablet_id=? ORDER BY recorded_at DESC LIMIT ?",
    [tabletId, limit]
  );
  res.json({ tabletId, logs: rows || [], count: rows?.length || 0 });
});

// ============ REST API: TABLET COMMANDS ============
app.post("/api/tablet/:tabletId/config", (req, res) => {
  const { tabletId } = req.params;
  const { condition, playlist } = req.body;
  const targets =
    tabletId === "ALL"
      ? Object.keys(connectedTablets)
      : connectedTablets[tabletId]
      ? [tabletId]
      : [];
  if (targets.length === 0)
    return res.json({
      error: "Tablet not found",
      connectedTablets: Object.keys(connectedTablets),
    });
  for (const id of targets) {
    publish(id, "update_config", { condition: condition || false });
    if (playlist)
      publish(id, "update_playlist", {
        type: condition ? "condition" : "default",
        playlist,
      });

    // Update DB
    dbQuery(
      "UPDATE tablets SET current_playlist_type=?, last_seen=NOW() WHERE tablet_id=?",
      [condition ? "condition" : "default", id]
    );
    console.log(`📤 Config sent to ${id}: condition=${condition}`);
  }
  res.json({ status: "sent", targets, condition });
});

app.post("/api/tablet/:tabletId/download", (req, res) => {
  const { tabletId } = req.params;
  const { videos } = req.body;
  if (!videos || !Array.isArray(videos))
    return res.status(400).json({ error: "videos must be an array" });
  if (!connectedTablets[tabletId])
    return res.json({ error: "Tablet not connected" });
  publish(
    tabletId,
    "download_videos",
    videos.map((v) => ({ video: v }))
  );
  console.log(`📥 Download list sent to ${tabletId}: ${videos.length} files`);
  res.json({ status: "sent", tabletId, videos, count: videos.length });
});

app.post("/api/tablet/:tabletId/delete", (req, res) => {
  const { tabletId } = req.params;
  const { files } = req.body;
  if (!connectedTablets[tabletId])
    return res.json({ error: "Tablet not connected" });
  publish(tabletId, "delete_videos", { files });
  res.json({ status: "sent", tabletId, files });
});

// ============ CRUD: VIDEOS ============
app.post("/api/videos", async (req, res) => {
  const {
    video_id,
    video_name,
    cloud_url,
    image_cover,
    image_promo,
    duration,
    size_mb,
  } = req.body;
  if (!video_name)
    return res.status(400).json({ error: "video_name required" });

  const finalVideoId = video_id || `vid_${Date.now()}`;

  // Check duplicate by video_name
  const existing = await dbQuery(
    "SELECT video_id, video_name FROM videos WHERE video_name=?",
    [video_name]
  );
  if (existing && existing.length > 0) {
    return res.status(409).json({
      error: "Duplicate video_name",
      video_name,
      existing_video_id: existing[0].video_id,
      message: "Video with this name already exists.",
    });
  }

  await dbQuery(
    `INSERT INTO videos (video_id, video_name, cloud_url, image_cover, image_promo, duration, size_mb)
     VALUES (?,?,?,?,?,?,?)`,
    [
      finalVideoId,
      video_name,
      cloud_url || null,
      image_cover || null,
      image_promo || null,
      duration || null,
      size_mb || null,
    ]
  );
  res.json({ status: "ok", video_id: finalVideoId });
});

app.get("/api/videos", async (req, res) => {
  const status = req.query.status || null;
  let sql = "SELECT * FROM videos";
  const params = [];
  if (status) {
    sql += " WHERE status=?";
    params.push(status);
  }
  sql += " ORDER BY id DESC";
  const rows = await dbQuery(sql, params);
  res.json({ videos: rows || [], count: rows?.length || 0 });
});

app.get("/api/videos/:videoId", async (req, res) => {
  const rows = await dbQuery("SELECT * FROM videos WHERE video_id=?", [
    req.params.videoId,
  ]);
  if (!rows || rows.length === 0)
    return res.status(404).json({ error: "Video not found" });
  res.json(rows[0]);
});

app.put("/api/videos/:videoId", async (req, res) => {
  const {
    video_name,
    cloud_url,
    image_cover,
    image_promo,
    duration,
    size_mb,
    status,
  } = req.body;
  const fields = [];
  const params = [];
  if (video_name !== undefined) {
    fields.push("video_name=?");
    params.push(video_name);
  }
  if (cloud_url !== undefined) {
    fields.push("cloud_url=?");
    params.push(cloud_url);
  }
  if (image_cover !== undefined) {
    fields.push("image_cover=?");
    params.push(image_cover);
  }
  if (image_promo !== undefined) {
    fields.push("image_promo=?");
    params.push(image_promo);
  }
  if (duration !== undefined) {
    fields.push("duration=?");
    params.push(duration);
  }
  if (size_mb !== undefined) {
    fields.push("size_mb=?");
    params.push(size_mb);
  }
  if (status !== undefined) {
    fields.push("status=?");
    params.push(status);
  }
  if (fields.length === 0)
    return res.status(400).json({ error: "No fields to update" });
  params.push(req.params.videoId);
  await dbQuery(
    `UPDATE videos SET ${fields.join(",")} WHERE video_id=?`,
    params
  );
  res.json({ status: "updated", video_id: req.params.videoId });
});

app.delete("/api/videos/:videoId", async (req, res) => {
  await dbQuery("DELETE FROM videos WHERE video_id=?", [req.params.videoId]);
  res.json({ status: "deleted", video_id: req.params.videoId });
});

// ============ UPLOAD: VIDEO + IMAGES ============
app.post(
  "/api/videos/upload",
  upload.fields([
    { name: "video", maxCount: 1 },
    { name: "image_cover", maxCount: 1 },
    { name: "image_promo", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const { video_id, video_name, duration, size_mb } = req.body;
      const finalVideoId = video_id || `vid_${req._uploadTimestamp}`;
      const finalVideoName =
        video_name || req.files?.video?.[0]?.originalname || finalVideoId;

      // Check duplicate by video_name
      const existing = await dbQuery(
        "SELECT video_id, video_name FROM videos WHERE video_name=?",
        [finalVideoName]
      );
      if (existing && existing.length > 0) {
        // Cleanup uploaded files
        if (req.files?.video?.[0]) fs.unlinkSync(req.files.video[0].path);
        if (req.files?.image_cover?.[0])
          fs.unlinkSync(req.files.image_cover[0].path);
        if (req.files?.image_promo?.[0])
          fs.unlinkSync(req.files.image_promo[0].path);
        return res.status(409).json({
          error: "Duplicate video_name",
          video_name: finalVideoName,
          existing_video_id: existing[0].video_id,
          message: "Video with this name already exists.",
        });
      }

      const baseUrl = `${req.protocol}://${req.get("host")}`;
      let cloud_url = req.body.cloud_url || null;
      let image_cover = req.body.image_cover_url || null;
      let image_promo = req.body.image_promo_url || null;

      if (req.files?.video?.[0])
        cloud_url = `${baseUrl}/videos/${req.files.video[0].filename}`;
      if (req.files?.image_cover?.[0])
        image_cover = `${baseUrl}/uploads/covers/${req.files.image_cover[0].filename}`;
      if (req.files?.image_promo?.[0])
        image_promo = `${baseUrl}/uploads/promos/${req.files.image_promo[0].filename}`;

      await dbQuery(
        `INSERT INTO videos (video_id, video_name, cloud_url, image_cover, image_promo, duration, size_mb)
         VALUES (?,?,?,?,?,?,?)`,
        [
          finalVideoId,
          finalVideoName,
          cloud_url,
          image_cover,
          image_promo,
          duration || null,
          size_mb || null,
        ]
      );

      res.json({
        status: "uploaded",
        video_id: finalVideoId,
        video_name: finalVideoName,
        urls: { cloud_url, image_cover, image_promo },
        files: {
          video: req.files?.video?.[0]?.filename || null,
          cover: req.files?.image_cover?.[0]?.filename || null,
          promo: req.files?.image_promo?.[0]?.filename || null,
        },
      });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  }
);

app.post(
  "/api/videos/:videoId/images",
  upload.fields([
    { name: "image_cover", maxCount: 1 },
    { name: "image_promo", maxCount: 1 },
  ]),
  async (req, res) => {
    try {
      const { videoId } = req.params;
      const baseUrl = `${req.protocol}://${req.get("host")}`;
      const updates = [];
      const params = [];

      if (req.files?.image_cover?.[0]) {
        updates.push("image_cover=?");
        params.push(
          `${baseUrl}/uploads/covers/${req.files.image_cover[0].filename}`
        );
      }
      if (req.files?.image_promo?.[0]) {
        updates.push("image_promo=?");
        params.push(
          `${baseUrl}/uploads/promos/${req.files.image_promo[0].filename}`
        );
      }

      if (updates.length === 0)
        return res.status(400).json({ error: "No images uploaded" });

      params.push(videoId);
      await dbQuery(
        `UPDATE videos SET ${updates.join(",")} WHERE video_id=?`,
        params
      );
      res.json({ status: "updated", video_id: videoId });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  }
);
// ============ CRUD: TABLET MENU ============
app.post(
  "/api/menu",
  upload.fields([{ name: "cover_image", maxCount: 1 }]),
  async (req, res) => {
    try {
      const { web_url, header, body: bodyText } = req.body;
      const baseUrl = `${req.protocol}://${req.get("host")}`;
      let cover_image_url = req.body.cover_image_url || null;

      if (req.files?.cover_image?.[0]) {
        cover_image_url = `${baseUrl}/uploads/menuCover/${req.files.cover_image[0].filename}`;
      }

      const result = await dbQuery(
        `INSERT INTO tablet_menu (cover_image_url, web_url, header, body) VALUES (?,?,?,?)`,
        [cover_image_url, web_url || null, header || null, bodyText || null]
      );

      res.json({
        status: "created",
        id: result?.insertId,
        cover_image_url,
        web_url,
        header,
      });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  }
);

app.get("/api/menu", async (_, res) => {
  const rows = await dbQuery(
    "SELECT * FROM tablet_menu WHERE active=1 ORDER BY id DESC"
  );
  res.json({ menu: rows || [], count: rows?.length || 0 });
});

app.get("/api/menu/:id", async (req, res) => {
  const rows = await dbQuery("SELECT * FROM tablet_menu WHERE id=?", [
    req.params.id,
  ]);
  if (!rows || rows.length === 0)
    return res.status(404).json({ error: "Menu not found" });
  res.json(rows[0]);
});

app.put(
  "/api/menu/:id",
  upload.fields([{ name: "cover_image", maxCount: 1 }]),
  async (req, res) => {
    try {
      const { web_url, header, body: bodyText, active } = req.body;
      const baseUrl = `${req.protocol}://${req.get("host")}`;
      const fields = [];
      const params = [];

      if (req.files?.cover_image?.[0]) {
        fields.push("cover_image_url=?");
        params.push(
          `${baseUrl}/uploads/menuCover/${req.files.cover_image[0].filename}`
        );
      } else if (req.body.cover_image_url !== undefined) {
        fields.push("cover_image_url=?");
        params.push(req.body.cover_image_url);
      }
      if (web_url !== undefined) {
        fields.push("web_url=?");
        params.push(web_url);
      }
      if (header !== undefined) {
        fields.push("header=?");
        params.push(header);
      }
      if (bodyText !== undefined) {
        fields.push("body=?");
        params.push(bodyText);
      }
      if (active !== undefined) {
        fields.push("active=?");
        params.push(active);
      }

      if (fields.length === 0)
        return res.status(400).json({ error: "No fields to update" });

      params.push(req.params.id);
      await dbQuery(
        `UPDATE tablet_menu SET ${fields.join(",")} WHERE id=?`,
        params
      );
      res.json({ status: "updated", id: req.params.id });
    } catch (e) {
      res.status(500).json({ error: e.message });
    }
  }
);

app.delete("/api/menu/:id", async (req, res) => {
  await dbQuery("DELETE FROM tablet_menu WHERE id=?", [req.params.id]);
  res.json({ status: "deleted", id: req.params.id });
});

// ============ START SERVER ============
const PORT = process.env.PORT || 9358;
server.listen(PORT, () => {
  console.log(`🚀 Server on port ${PORT}`);
  console.log(`📡 MQTT: ${ENV} → ${mqttConfig.url}`);
  console.log(`🗄️ DB: ${pool ? "connected" : "skipped"}`);
});

process.on("SIGINT", async () => {
  mqttClient.end();
  if (pool) await pool.end();
  process.exit(0);
});
