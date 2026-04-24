const mongoose = require("mongoose");

let isConnected = false;

const connectDB = async () => {
  if (isConnected) {
    return mongoose.connection;
  }

  const uri = process.env.MONGODB_URI;
  if (!uri) {
    throw new Error("Missing MONGODB_URI in environment.");
  }

  await mongoose.connect(uri, {
    autoIndex: true
  });

  isConnected = true;
  return mongoose.connection;
};

module.exports = {
  connectDB
};
