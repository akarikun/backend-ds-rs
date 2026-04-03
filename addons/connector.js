query("get_player", async (db, data) => {
  // console.log(data);
  let rows = await db.query(
    "select * from players where userid = ?",
    [data.userid || ""]
  );
  return rows[0] || null;
});
