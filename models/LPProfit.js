import mongoose from "mongoose";

const LPProfitSchema = new mongoose.Schema(
  {
    orderNo: {
      type: String,
      required: true,
    },
    orderType: {
      type: String,
      enum: ["BUY", "SELL"],
      required: true,
    },
    status: {
      type: String,
      enum: ["OPEN", "CLOSED"],
      default: "OPEN",
    },
    volume: {
      type: Number,
      required: true,
    },
    value: {
      type: Number,
      required: true,
    },
    user: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Account",
      required: true,
    },
    datetime: {
      type: Date,
      default: Date.now,
    },
  },
  { timestamps: true }
);

const LPProfit = mongoose.model("LPProfit", LPProfitSchema);

export default LPProfit;