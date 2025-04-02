db = db.getSiblingDB("seminar");
db.createUser({
  user: "seminar",
  pwd: "seminar",
  roles: [{ role: "readWrite", db: "seminar" }],
});
