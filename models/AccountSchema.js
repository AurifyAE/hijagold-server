import mongoose from "mongoose";

const AccountSchema = new mongoose.Schema(
  {
    REFMID: {
      type: Number,
      required: true,
    },
    ACCOUNT_HEAD: {
      type: String,
      required: true,
    },
    ACCODE: {
      type: String,
      required: true,
    },
    AMOUNTFC: {
      type: Number,
      required: true,
    },
    reservedAmount: {
      type: Number,
      required: true,
    },
    METAL_WT: {
      type: Number,
      default: 0,
    },
    margin: {
      type: Number,
      default: 0,
    },
    symbol: {
      type: String,
      required: true,
    },
    isFreeze: {
      type: Boolean,
      default: false,
    },
    is_favorite: {
      type: Boolean,
      default: false,
    },
    created_by: {
      type: String,
      default: "admin",
    },
    addedBy: {
      type: mongoose.Schema.Types.ObjectId,
      ref: "Admin",
      default: null,
    },
    firstName: {
      type: String,
      default: null,
    },
    lastName: {
      type: String,
      default: null,
    },
    email: {
      type: String,
      default: null,
      sparse: true,
    },
    phoneNumber: {
      type: String,
      default: null,
      // required: true,
      match: [
        /^\+\d{10,15}$/,
        "Phone number must be in E.164 format (e.g., +918138823410)",
      ],
      // unique: true,
    },
    address: {
      street: { type: String, default: null },
      city: { type: String, default: null },
      state: { type: String, default: null },
      country: { type: String, default: null },
      zipCode: { type: String, default: null },
    },
    joinDate: {
      type: Date,
      default: Date.now,
    },
    accountStatus: {
      type: String,
      enum: ["active", "inactive", "suspended", "pending"],
      default: "pending",
    },
    kycStatus: {
      type: String,
      enum: ["verified", "pending", "rejected", "not_submitted"],
      default: "not_submitted",
    },
    askSpread: {
      type: Number,
      default: 0,
    },
    bidSpread: {
      type: Number,
      default: 0,
    },
    lastUpdated: { type: Date, default: null },
  },
  { timestamps: true }
);

const Account = mongoose.model("Account", AccountSchema);

export default Account;
