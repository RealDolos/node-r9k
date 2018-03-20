"use strict";

const {spawn} = require("child_process");
const crypto = require("crypto");
const LevelUp = require("levelup");
const LevelDown = require("leveldown");
const {promisify} = require("util");

const {PromisePool} = require.main.require("./parrot/pool");
const {FileCommand} = require.main.require("./commands/command");

const POOL_LIMIT = 2;
const SIZE_SMALL_LIMIT = 50;
const SIZE_EXIF_LIMIT_MB = 10;
const SIZE_EXIF_LIMIT = SIZE_EXIF_LIMIT_MB << 20;

const OK = new Set([]);

async function fetch_file_exif(file) {
  const resp = await file.fetch();
  let hash = crypto.createHash("sha256");
  const exiftool = spawn("exiftool", ["-all=", "-"], {
    stdio: ["pipe", "pipe", "ignore"],
  });

  return await new Promise((resolve, reject) => {
    exiftool.on("error", reject);
    resp.body.pipe(exiftool.stdin).on("error", reject);
    const pipe = exiftool.stdout.pipe(hash);
    pipe.on("error", reject);
    exiftool.on("exit", code => {
      if (code) {
        reject(new Error(`no dice: ${code}`));
      }
      else {
        resolve(hash);
      }
    });
    pipe.on("finish", () => hash = hash.read());
  });
}

const DBS = new class DBs extends Map {
  get(key) {
    let rv = super.get(key);
    if (rv) {
      return rv;
    }

    const db = new LevelUp(new LevelDown(key));
    const get = promisify(db.get.bind(db));
    const put = promisify(db.put.bind(db));
    rv = {get, put};
    super.set(key, rv);
    return rv;
  }
}();

async function getHash(file) {
  try {
    if (file.type !== "image" || file.size > SIZE_EXIF_LIMIT) {
      const err = new Error("Not exifing");
      err.skipped = true;
      throw err;
    }
    return await fetch_file_exif(file);
  }
  catch (ex) {
    if (!("skipped" in ex)) {
      console.error(ex.message || ex, file);
    }
    const hash = (await file.infos()).checksum;
    if (!hash) {
      throw new Error("no hash");
    }
    return hash;
  }
}

class R9K extends FileCommand {
  constructor(...args) {
    super(...args);
    this.onfile = PromisePool.wrapNew(POOL_LIMIT, this, this.onfile);
  }

  async onfile(room, file) {
    if (file.size <= SIZE_SMALL_LIMIT) {
      console.info("skipping".yellow, file);
      return;
    }
    const db = DBS.get(`${__dirname}/${room.id}.hashes`);
    try {
      await db.get(file.id);
      return;
    }
    catch (ex) {
      if (!ex.notFound) {
        throw ex;
      }
    }
    console.info("processing".yellow, file);
    try {
      const hash = await getHash(file);
      console.debug("hash", file.id, hash);
      try {
        if (OK.has(hash)) {
          return;
        }
        const existing = await db.get(hash);
        if (!existing || existing === file.id) {
          await db.put(file.id, true);
          console.info("same".yellow, file);
          return;
        }
        console.info("GOTCHA!".bold.red, file);
        if (room.owner) {
          file.timeout(5);
        }
        else if (room.privileged && file.ip) {
          room.ban(file.ip, {
            hours: 0.1,
            reason: "Dupe",
            ban: true
          });
        }
        room.chat(`${file.uploader}: pls, no dupes, not even @${file.id}`);
        file.delete();
      }
      catch (ex) {
        if (ex.notFound) {
          await db.put(hash, file.id);
          await db.put(file.id, true);
          console.info("added".bold.white, file);
          return;
        }
        throw ex;
      }
    }
    catch (ex) {
      console.error(ex);
    }
  }
}

module.exports = (handler, options) => {
  handler.registerFileCommand(new R9K(options));
};
