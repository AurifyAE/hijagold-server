import mongoose from "mongoose";

const LiquidityProviderSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: true,
    },
    code: {
      type: String,
      required: true,
      unique: true,
    },
    connectionType: {
      type: String,
      enum: ["DIRECT", "BRIDGE", "API"],
      default: "API"
    },
    isActive: {
      type: Boolean,
      default: true,
    },
    balance: {
      type: Number,
      default: 0,
    },
    apiKeys: {
      apiKey: String,
      secretKey: String,
      passphrase: String
    },
    settings: {
      maxExposure: {
        type: Number,
        default: 0
      },
      autoHedge: {
        type: Boolean,
        default: false
      },
      hedgeThreshold: {
        type: Number,
        default: 0
      }
    },
    connectionStatus: {
      type: String,
      enum: ["CONNECTED", "DISCONNECTED", "ERROR"],
      default: "DISCONNECTED"
    },
    lastConnected: {
      type: Date,
      default: null
    }
  },
  { timestamps: true }
);

const LiquidityProvider = mongoose.model("LiquidityProvider", LiquidityProviderSchema);

export default LiquidityProvider;