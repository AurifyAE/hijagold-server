import * as adminServices from "../../services/superAdmin/adminServices.js";

export const registerAdmin = async (req, res, next) => {
  try {
    const { userName, email, password, contact, features } = req.body;
    
    // Validate required fields
    if (!userName || !email || !password || !contact) {
      return res.status(400).json({
        success: false,
        message: "All fields are required"
      });
    }
    
    const newAdmin = await adminServices.registerAdmin({
      userName,
      email,
      password,
      contact,
      features
    });
    
    res.status(201).json({
      success: true,
      message: "Admin registered successfully",
      data: newAdmin
    });
  } catch (error) {
    next(error);
  }
};

export const loginAdmin = async (req, res, next) => {
  try {
    const { userName, password,rememberMe } = req.body;
    
    if (!userName || !password) {
      return res.status(400).json({
        success: false,
        message: "Username and password are required"
      });
    }
    
    const result = await adminServices.loginAdmin({ userName, password,rememberMe });
    
    res.status(200).json({
      success: true,
      message: "Login successful",
      data: {
        admin: result.admin,
        token: result.token
      }
    });
  } catch (error) {
    next(error);
  }
};

export const getAdminProfile = async (req, res, next) => {
  try {
    const { adminId } = req.params;
    const admin = await adminServices.getAdminById(adminId);
    
    res.status(200).json({
      success: true,
      data: admin
    });
  } catch (error) {
    next(error);
  }
};

export const updateAdminProfile = async (req, res, next) => {
  try {
    const adminId = req.params.id;
    
    // Update admin data
    const updatedAdmin = await adminServices.updateAdmin(adminId, req.body);
    
    res.status(200).json({
      success: true,
      message: "Admin profile updated successfully",
      data: updatedAdmin
    });
  } catch (error) {
    next(error);
  }
};

export const getAllAdmins = async (req, res, next) => {
  try {
    const admins = await adminServices.getAllAdmins();
    
    res.status(200).json({
      success: true,
      count: admins.length,
      data: admins
    });
  } catch (error) {
    next(error);
  }
};

export const updateAdminById = async (req, res, next) => {
  try {
    const { adminId } = req.params;
    
    // Update admin data
    const updatedAdmin = await adminServices.updateAdmin(adminId, req.body);
    
    res.status(200).json({
      success: true,
      message: "Admin updated successfully",
      data: updatedAdmin
    });
  } catch (error) {
    next(error);
  }
};

export const deleteAdminById = async (req, res, next) => {
  try {
    const { adminId } = req.params;
    
    // Delete admin
    await adminServices.deleteAdmin(adminId);
    
    res.status(200).json({
      success: true,
      message: "Admin deleted successfully"
    });
  } catch (error) {
    next(error);
  }
};