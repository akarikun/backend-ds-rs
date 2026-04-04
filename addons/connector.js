function isDbError(value) {
  return !!value && value.ok === false && typeof value.error === "string";
}

function normalizeCount(value, fallback) {
  let num = Number(value);
  if (!Number.isFinite(num)) {
    return fallback;
  }
  return Math.trunc(num);
}

function mailQueryOf(userid, mailId) {
  let key = String(mailId || "").trim();
  let numId = Number(key);
  if (key && Number.isFinite(numId) && String(Math.trunc(numId)) === key) {
    return Math.trunc(numId);
  }

  return 0;
}

function playerCacheKey(userid) {
  return `player:${userid}`;
}

async function loadPlayer(db, userid, reloadCache) {
  if (!reloadCache) {
    let cachedPlayer = await cache.get(playerCacheKey(userid));
    if (cachedPlayer && !isDbError(cachedPlayer)) {
      return {
        ...cachedPlayer,
        cache_hit: true,
      };
    }
  }

  let profiles = await db.query(
    "select * from player_profiles where userid = ?",
    [userid]
  );
  if (isDbError(profiles)) {
    return profiles;
  }
  let profile = profiles[0];
  if (!profile) {
    await cache.del(playerCacheKey(userid));
    return null;
  }

  let wallets = await db.query(
    "select * from player_wallets where userid = ?",
    [userid]
  );
  if (isDbError(wallets)) {
    return wallets;
  }
  let items = await db.query(
    "select * from player_items where userid = ? order by id asc",
    [userid]
  );
  if (isDbError(items)) {
    return items;
  }
  let mails = await db.query(
    "select * from player_mails where userid = ? order by id desc",
    [userid]
  );
  if (isDbError(mails)) {
    return mails;
  }

  let player = {
    profile,
    wallet: wallets[0] || {
      userid,
      gold: 0,
      diamond: 0,
      stamina: 100,
      updated_at: profile.updated_at,
    },
    items,
    mails,
  };
  await cache.set(playerCacheKey(userid), player);
  return player;
}

async function ensurePlayer(db, userid, type) {
  if (!userid) {
    return {
      ok: false,
      type,
      error: "userid is empty",
    };
  }

  let player = await loadPlayer(db, userid);
  if (isDbError(player)) {
    return {
      ok: false,
      type,
      error: player.error,
    };
  }
  if (!player) {
    return {
      ok: false,
      type,
      error: "player_not_found",
    };
  }

  return {
    ok: true,
    player,
  };
}

async function applyAttachment(db, userid, attachment, type) {
  let kind = attachment.kind || "";
  let count = normalizeCount(attachment.count, 0);
  if (count <= 0) {
    return {
      ok: false,
      type,
      error: "attachment count must be positive",
    };
  }

  if (kind === "currency") {
    return await changeCurrency(db, userid, attachment.name || "", count, type);
  }
  if (kind === "item") {
    return await changeItem(db, userid, attachment.item_id || "", count, type);
  }

  return {
    ok: false,
    type,
    error: `unsupported attachment kind: ${kind}`,
  };
}

async function changeCurrency(db, userid, currency, delta, type) {
  if (currency !== "gold" && currency !== "diamond" && currency !== "stamina") {
    return {
      ok: false,
      type,
      error: `unsupported currency: ${currency}`,
    };
  }

  let checked = await ensurePlayer(db, userid, type);
  if (!checked.ok) {
    return checked;
  }

  let wallet = checked.player.wallet || {};
  let nextValue = normalizeCount(wallet[currency], 0) + delta;
  if (nextValue < 0) {
    return {
      ok: false,
      type,
      error: `${currency} is not enough`,
    };
  }

  let updated = await db.query(
    `update player_wallets
     set ${currency} = ?, updated_at = now()
     where userid = ?
     returning *`,
    [nextValue, userid]
  );
  if (isDbError(updated)) {
    return {
      ok: false,
      type,
      error: updated.error,
    };
  }

  return {
    ok: true,
    type,
    wallet: updated[0] || { ...wallet, [currency]: nextValue },
    player: await loadPlayer(db, userid, true),
  };
}

async function changeItem(db, userid, itemId, delta, type) {
  if (!itemId) {
    return {
      ok: false,
      type,
      error: "item_id is empty",
    };
  }

  let checked = await ensurePlayer(db, userid, type);
  if (!checked.ok) {
    return checked;
  }

  let rows = await db.query(
    "select * from player_items where userid = ? and item_id = ?",
    [userid, itemId]
  );
  if (isDbError(rows)) {
    return {
      ok: false,
      type,
      error: rows.error,
    };
  }

  let item = rows[0];
  let nextCount = normalizeCount(item ? item.item_count : 0, 0) + delta;
  if (nextCount < 0) {
    return {
      ok: false,
      type,
      error: "item_count is not enough",
    };
  }

  if (item) {
    let updated = await db.query(
      `update player_items
       set item_count = ?, updated_at = now()
       where userid = ? and item_id = ?
       returning *`,
      [nextCount, userid, itemId]
    );
    if (isDbError(updated)) {
      return {
        ok: false,
        type,
        error: updated.error,
      };
    }
  } else if (nextCount > 0) {
    let inserted = await db.query(
      `insert into player_items (userid, item_id, item_count)
       values (?, ?, ?)
       returning *`,
      [userid, itemId, nextCount]
    );
    if (isDbError(inserted)) {
      return {
        ok: false,
        type,
        error: inserted.error,
      };
    }
  }

  return {
    ok: true,
    type,
    item: {
      userid,
      item_id: itemId,
      item_count: nextCount,
    },
    player: await loadPlayer(db, userid, true),
  };
}

query("create_player", async (db, data) => {
  let userid = data.userid || "";
  let nickname = data.nickname || "player";
  if (!userid) {
    return {
      ok: false,
      type: "create_player",
      error: "userid is empty",
    };
  }

  let existsPlayer = await loadPlayer(db, userid);
  if (isDbError(existsPlayer)) {
    return {
      ok: false,
      type: "create_player",
      error: existsPlayer.error,
    };
  }
  if (existsPlayer) {
    return {
      ok: true,
      type: "create_player",
      player: existsPlayer,
    };
  }

  let profile = await db.query(
    `insert into player_profiles (userid, nickname, level, exp, avatar_id)
     values (?, ?, ?, ?, ?)
     on conflict (userid)
     do update set nickname = excluded.nickname, updated_at = now()
     returning *`,
    [userid, nickname, 1, 0, 0]
  );
  if (profile && profile.ok === false) {
    return {
      ok: false,
      type: "create_player",
      error: profile.error || "create profile failed",
    };
  }

  let wallet = await db.query(
    `insert into player_wallets (userid, gold, diamond, stamina)
     values (?, ?, ?, ?)
     on conflict (userid)
     do update set updated_at = now()
     returning *`,
    [userid, 0, 0, 100]
  );
  if (wallet && wallet.ok === false) {
    return {
      ok: false,
      type: "create_player",
      error: wallet.error || "create wallet failed",
    };
  }

  let reloadPlayer = await loadPlayer(db, userid, true);

  return {
    ok: true,
    type: "create_player",
    player: reloadPlayer,
    debug: {
      userid,
      profile_insert: profile,
      wallet_insert: wallet,
      reload_player: reloadPlayer,
    },
  };
});

query("get_player", async (db, data) => {
  let userid = data.userid || "";
  if (!userid) {
    return {
      ok: false,
      type: "get_player",
      error: "userid is empty",
    };
  }

  let player = await loadPlayer(db, userid);
  if (isDbError(player)) {
    return {
      ok: false,
      type: "get_player",
      error: player.error,
    };
  }
  if (!player) {
    return {
      ok: false,
      type: "get_player",
      error: "player_not_found",
    };
  }

  return {
    ok: true,
    type: "get_player",
    player,
  };
});

query("add_currency", async (db, data) => {
  return await changeCurrency(
    db,
    data.userid || "",
    data.currency || "",
    normalizeCount(data.delta, 0),
    "add_currency"
  );
});

query("add_item", async (db, data) => {
  return await changeItem(
    db,
    data.userid || "",
    data.item_id || "",
    normalizeCount(data.delta, 0),
    "add_item"
  );
});

query("send_mail", async (db, data) => {
  let userid = data.userid || "";
  let checked = await ensurePlayer(db, userid, "send_mail");
  if (!checked.ok) {
    return checked;
  }

  let mail = await db.query(
    `insert into player_mails (userid, title, content, attachments, status)
     values (?, ?, ?, ?, ?)
     returning *`,
    [
      userid,
      data.title || "system mail",
      data.content || "",
      Array.isArray(data.attachments) ? data.attachments : [],
      "unread"
    ]
  );
  if (isDbError(mail)) {
    return {
      ok: false,
      type: "send_mail",
      error: mail.error,
    };
  }

  return {
    ok: true,
    type: "send_mail",
    mail: mail[0] || null,
    player: await loadPlayer(db, userid, true),
  };
});

query("claim_mail", async (db, data) => {
  let userid = data.userid || "";
  let checked = await ensurePlayer(db, userid, "claim_mail");
  if (!checked.ok) {
    return checked;
  }

  let mailId = mailQueryOf(userid, data.mail_id);
  let mailRows = await db.query(
    "select * from player_mails where id = ? and userid = ?",
    [mailId, userid]
  );
  if (isDbError(mailRows)) {
    return {
      ok: false,
      type: "claim_mail",
      error: mailRows.error,
    };
  }

  let mail = mailRows[0];
  if (!mail) {
    return {
      ok: false,
      type: "claim_mail",
      error: "mail_not_found",
    };
  }
  if (mail.status === "claimed") {
    return {
      ok: false,
      type: "claim_mail",
      error: "mail_already_claimed",
    };
  }

  let attachments = Array.isArray(mail.attachments) ? mail.attachments : [];
  for (let attachment of attachments) {
    let applied = await applyAttachment(db, userid, attachment, "claim_mail");
    if (!applied.ok) {
      return applied;
    }
  }

  let updated = await db.query(
    `update player_mails
     set status = ?
     where id = ? and userid = ?
     returning *`,
    ["claimed", mailId, userid]
  );
  if (isDbError(updated)) {
    return {
      ok: false,
      type: "claim_mail",
      error: updated.error,
    };
  }

  return {
    ok: true,
    type: "claim_mail",
    mail: updated[0] || { ...mail, status: "claimed" },
    player: await loadPlayer(db, userid, true),
  };
});
