import mongoose from "mongoose";
import mongoosePaginate from "mongoose-paginate-v2";

const LedgerSchema = new mongoose.Schema(
  {
    entryId: {
      type: String,
      unique: true,
      required: true,
    },
    entryType: {
      type: String,
    //   enum: ["ORDER", "TRANSACTION", "ADJUSTMENT", "LP_POSITION"],
      required: true,
    },
    referenceNumber: {
      type: String,
      required: true,
    },
    description: {
      type: String,
      required: true,
      default: null,
    },
    amount: {
      type: Number,
      required: true,
    },
    entryNature: {
      type: String,
      enum: ["DEBIT", "CREDIT"],
      required: true,
    },
    runningBalance: {
      type: Number,
      required: true,
    },
    transactionDetails: {
      type: {
        type: String,
        enum: ["DEPOSIT", "WITHDRAWAL", null],
        default: null,
      },
      asset: {
        type: String,
        enum: ["GOLD", "CASH", null],
        default: null,
      },
      previousBalance: {
        type: Number,
        default: null,
      },
    },
    orderDetails: {
      type: {
        type: String,
        enum: ["BUY", "SELL", null],
        default: null,
      },
      symbol: {
        type: String,
        default: null,
      },
      volume: {
        type: Number,
        default: null,
      },
      entryPrice: {
        type: Number,
        default: null,
      },
      closingPrice: {
        type: Number,
        default: null,
      },
      profit: {
        type: Number,
        default: null,
      },
      status: {
        type: String,
        enum: [
          "PROCESSING",
          "EXECUTED",
          "CANCELLED",
          "CLOSED",
          "PENDING",
          null,
        ],
        default: null,
      },
    },
    lpDetails: {
      positionId: {
        type: String,
        default: null,
      },
      type: {
        type: String,
        enum: ["BUY", "SELL", null],
        default: null,
      },
      symbol: {
        type: String,
        default: null,
      },
      volume: {
        type: Number,
        default: null,
      },
      entryPrice: {
        type: Number,
        default: null,
      },
      closingPrice: {
        type: Number,
        default: null,
      },
      profit: {
        type: Number,
        default: null,
      },
      status: {
        type: String,
        enum: ["OPEN", "CLOSED", "PARTIALLY_CLOSED", null],
        default: null,
      },
    },
    date: {
      type: Date,
      default: Date.now,
      required: true,
    },
    user: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Account",
      required: true,
    },
    adminId: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Admin",
      required: true,
    },
    notes: {
      type: String,
      default: "",
    },
  },
  { timestamps: true }
);

// Apply the mongoose-paginate-v2 plugin to enable pagination
LedgerSchema.plugin(mongoosePaginate);

const Ledger = mongoose.model("Ledger", LedgerSchema);

export default Ledger;