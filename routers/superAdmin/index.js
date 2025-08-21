import express from "express";
import { 
  registerAdmin,
  loginAdmin,
  updateAdminProfile,
  deleteAdminById,
  getAdminProfile
} from "../../controllers/superAdmin/adminControllers.js";
import { protect } from "../../middleware/authMiddleware.js";

const router = express.Router();

router.post("/register", registerAdmin);
router.post("/login", loginAdmin);
router.put("/edit-admin/:id", updateAdminProfile);
router.delete("/delete-admin/:adminId", deleteAdminById);
router.get("/profile/:adminId", getAdminProfile);

export default router;