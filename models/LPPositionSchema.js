import mongoose from "mongoose";

const LPPositionSchema = new mongoose.Schema(
  {
    positionId: {
      type: String,
      unique: true,
      required: true,
    },
    type: {
      type: String,
      enum: ["BUY", "SELL"],
      required: true,
    },
    volume: {
      type: Number,
      required: true,
      min: 0.01,
    },
    symbol: {
      type: String,
      required: true,
    },
    entryPrice: {
      type: Number,
      required: true,
    },
    currentPrice: {
      type: Number,
      required: true,
    },
    closingPrice: {
      type: Number,
      default: null,
    },
    openDate: {
      type: Date,
      default: Date.now,
    },
    closeDate: {
      type: Date,
      default: null,
    },
    status: {
      type: String,
      enum: ["OPEN", "CLOSED", "PARTIALLY_CLOSED"],
      default: "OPEN",
    },
    profit: {
      type: Number,
      default: 0,
    },
    clientOrders: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Order",
    },
    adminId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Admin",
      required: true,
    },
    externalReference: {
      type: String,
      default: null,
    },
    notes: {
      type: String,
      default: "",
    },
  },
  { timestamps: true }
);

const LPPosition = mongoose.model("LPPosition", LPPositionSchema);
export default LPPosition;
