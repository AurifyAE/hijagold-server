import mongoose from "mongoose";

const adminSchema = new mongoose.Schema(
  {
    userName: { 
      type: String, 
      required: true, 
      unique: true 
    },
    email: { 
      type: String, 
      required: true, 
      unique: true 
    },
    password: { 
      type: String, 
      required: true 
    },
    passwordAccessKey: { 
      type: String, 
      required: true 
    },
    contact: { 
      type: String, 
      required: true 
    },
    // Added balance fields
    cashBalance: {
      type: Number,
      default: 0
    },
    goldBalance: {
      type: Number,
      default: 0
    },
    margin: {
      type: Number,
      default: 0
    },
    features: [
      {
        name: { 
          type: String 
        },
        enabled: { 
          type: Boolean, 
          default: true 
        },
      },
    ],
  },
  { timestamps: true }
);

const Admin = mongoose.model("Admin", adminSchema);

export default Admin;