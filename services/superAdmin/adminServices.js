import Admin from "../../models/AdminSchema.js";
import { createAppError } from "../../utils/errorHandler.js";
import { decryptPassword, encryptPassword } from "../../utils/crypto.js";
import { generateToken } from "../../utils/jwt.js";
import bcrypt from "bcrypt";

export const registerAdmin = async (adminData) => {
  try {
    const existingAdmin = await Admin.findOne({
      $or: [{ userName: adminData.userName }, { email: adminData.email }],
    });

    if (existingAdmin) {
      throw createAppError("Username or email already exists", 400);
    }

    const { iv, encryptedData } = encryptPassword(adminData.password);

    // Create new admin with hashed credentials
    const newAdmin = new Admin({
      ...adminData,
      password: encryptedData,
      passwordAccessKey: iv,
    });

    await newAdmin.save();

    // Return admin without sensitive information
    const adminToReturn = newAdmin.toObject();
    delete adminToReturn.password;
    delete adminToReturn.passwordAccessKey;

    return adminToReturn;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error registering admin: ${error.message}`, 500);
  }
};

export const loginAdmin = async (credentials) => {
  try {
    const { userName, password,rememberMe } = credentials;
    const admin = await Admin.findOne({ userName });
    if (!admin) {
      throw createAppError("Invalid credentials", 401);
    }
    const decryptedPassword = decryptPassword(
      admin.password,
      admin.passwordAccessKey
    );
    if (password !== decryptedPassword) {
      throw createAppError("Invalid credentials.", 401);
    }
    const expiresIn = rememberMe ? "30d" : "3d";
    const token = generateToken({ adminId: admin._id }, expiresIn);
    // Return admin data and token
    const adminToReturn = admin.toObject();
    delete adminToReturn.password;
    delete adminToReturn.passwordAccessKey;

    return {
      admin: adminToReturn,
      token,
    };
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Login error: ${error.message}`, 500);
  }
};

export const getAdminById = async (adminId) => {
  try {
    const admin = await Admin.findById(adminId).select(
      "-password -passwordAccessKey"
    );

    if (!admin) {
      throw createAppError("Admin not found", 404);
    }

    return admin;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error fetching admin: ${error.message}`, 500);
  }
};

export const updateAdmin = async (adminId, updateData) => {
  try {
    // Check if updating username or email to ensure they don't conflict with existing admins
    if (updateData.userName || updateData.email) {
      const existingAdmin = await Admin.findOne({
        _id: { $ne: adminId }, // Exclude the current admin
        $or: [
          { userName: updateData.userName },
          { email: updateData.email }
        ].filter(Boolean) // Only include conditions for fields being updated
      });

      if (existingAdmin) {
        throw createAppError("Username or email already exists", 400);
      }
    }

    // Handle password update if provided
    if (updateData.password) {
      const { iv, encryptedData } = encryptPassword(updateData.password);
      updateData.password = encryptedData;
      updateData.passwordAccessKey = iv;
    }

    // Update admin
    const updatedAdmin = await Admin.findByIdAndUpdate(
      adminId,
      { $set: updateData },
      { new: true, runValidators: true }
    ).select('-password -passwordAccessKey');

    if (!updatedAdmin) {
      throw createAppError("Admin not found", 404);
    }

    return updatedAdmin;
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error updating admin: ${error.message}`, 500);
  }
};


export const getAllAdmins = async () => {
  try {
    const admins = await Admin.find().select("-password -passwordAccessKey");
    return admins;
  } catch (error) {
    throw createAppError(`Error fetching admins: ${error.message}`, 500);
  }
};

export const deleteAdmin = async (adminId) => {
  try {
    const deletedAdmin = await Admin.findByIdAndDelete(adminId);

    if (!deletedAdmin) {
      throw createAppError("Admin not found", 404);
    }

    return { message: "Admin deleted successfully" };
  } catch (error) {
    if (error.statusCode) throw error;
    throw createAppError(`Error deleting admin: ${error.message}`, 500);
  }
};
