// authMiddleware.js
import jwt from "jsonwebtoken";
import { createAppError } from "../utils/errorHandler.js";
import Admin from "../models/AccountSchema.js";

export const protect = async (req, res, next) => {
  try {
    let token;
    
    // Check if token exists in headers
    if (
      req.headers.authorization &&
      req.headers.authorization.startsWith("Bearer")
    ) {
      token = req.headers.authorization.split(" ")[1];
    }
    
    if (!token) {
      return next(createAppError("Not authorized to access this route", 401));
    }
    
    try {
      // Verify token
      const decoded = jwt.verify(token, process.env.JWT_SECRET);
      
      // Check if admin still exists
      const admin = await Admin.findById(decoded.id).select("-password -passwordAccessKey");
      
      if (!admin) {
        return next(createAppError("Admin not found", 404));
      }
      
      // Add admin to request object
      req.adminId = admin._id;
      req.admin = admin;
      
      next();
    } catch (error) {
      return next(createAppError("Not authorized to access this route", 401));
    }
  } catch (error) {
    next(error);
  }
};